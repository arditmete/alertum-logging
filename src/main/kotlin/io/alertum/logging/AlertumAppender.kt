package io.alertum.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.StreamEntryID
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class AlertumAppender : AppenderBase<ILoggingEvent>() {

    @Volatile private var teamId: String = System.getenv("ALERTUM_TEAM_ID")?.trim().orEmpty()
    @Volatile private var service: String = AlertumDefaults.DEFAULT_SERVICE
    @Volatile private var environment: String = AlertumDefaults.DEFAULT_ENVIRONMENT

    @Volatile private var redisHost: String =
        System.getenv("ALERTUM_REDIS_HOST")?.trim()?.takeIf { it.isNotEmpty() } ?: DEFAULT_REDIS_HOST
    @Volatile private var redisPort: Int =
        System.getenv("ALERTUM_REDIS_PORT")?.toIntOrNull()?.takeIf { it > 0 } ?: DEFAULT_REDIS_PORT
    @Volatile private var redisPassword: String? =
        System.getenv("ALERTUM_REDIS_PASSWORD")?.trim()?.takeIf { it.isNotEmpty() }
    @Volatile private var streamKey: String =
        System.getenv("ALERTUM_REDIS_STREAM_KEY")?.trim()?.takeIf { it.isNotEmpty() } ?: DEFAULT_STREAM_KEY
    @Volatile private var queueCapacity: Int =
        System.getenv("ALERTUM_QUEUE_CAPACITY")?.toIntOrNull()?.takeIf { it > 0 } ?: DEFAULT_QUEUE_CAPACITY
    @Volatile private var dropWhenFull: Boolean =
        System.getenv("ALERTUM_DROP_WHEN_FULL")?.trim()?.lowercase()?.let { parseBooleanValue(it) } ?: true

    private val running = AtomicBoolean(false)

    @Volatile
    private var queue = LinkedBlockingQueue<ILoggingEvent>(queueCapacity)

    @Volatile
    private var workerThread: Thread? = null

    @Volatile
    private var jedisPool: JedisPool? = null

    private val droppedLogs = AtomicLong(0)
    private val sentLogs = AtomicLong(0)
    private val failedLogs = AtomicLong(0)
    private val lastWarnAt = AtomicLong(0)

    private val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    // ---------------- SETTERS ----------------

    fun setTeamId(value: String) {
        if (isStarted) return
        teamId = value.trim()
    }

    fun setService(value: String) {
        service = value.trim().ifEmpty { AlertumDefaults.DEFAULT_SERVICE }
    }

    fun setEnvironment(value: String) {
        environment = value.trim().ifBlank { AlertumDefaults.DEFAULT_ENVIRONMENT }
    }

    fun setRedisHost(value: String) {
        if (isStarted) return
        redisHost = value.trim().ifEmpty { DEFAULT_REDIS_HOST }
    }

    fun setRedisPort(value: Int) {
        if (isStarted) return
        if (value > 0) {
            redisPort = value
        }
    }

    fun setRedisPassword(value: String) {
        if (isStarted) return
        redisPassword = value.trim().ifEmpty { null }
    }

    fun setStreamKey(value: String) {
        if (isStarted) return
        streamKey = value.trim().ifEmpty { DEFAULT_STREAM_KEY }
    }

    fun setQueueCapacity(value: Int) {
        if (isStarted) return
        if (value > 0) {
            queueCapacity = value
        }
    }

    fun setDropWhenFull(value: Boolean) {
        dropWhenFull = value
    }

    fun getDroppedLogs(): Long = droppedLogs.get()

    fun getSentBatches(): Long = sentLogs.get()

    fun getFailedBatches(): Long = failedLogs.get()

    fun getQueueSize(): Int = queue.size

    // ---------------- LIFECYCLE ----------------
    override fun start() {
        if (isStarted) return

        val resolvedTeam = teamId.trim()
        if (resolvedTeam.isEmpty()) {
            addError("AlertumAppender failed to start: teamId is required")
            return
        }
        teamId = resolvedTeam

        queue = LinkedBlockingQueue(queueCapacity)

        val pool = try {
            JedisPool(buildPoolConfig(), redisHost, redisPort, REDIS_TIMEOUT_MS, redisPassword)
        } catch (ex: Exception) {
            addError("AlertumAppender failed to start: unable to initialize Redis pool", ex)
            return
        }
        jedisPool = pool

        super.start()

        if (running.compareAndSet(false, true)) {
            workerThread = Thread(::workerLoop, "alertum-redis-stream-worker").apply {
                isDaemon = true
                start()
            }
        }

        addInfo("AlertumAppender started (redis-streams) stream=$streamKey host=$redisHost:$redisPort")
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) {
            super.stop()
            return
        }

        workerThread?.interrupt()
        try {
            workerThread?.join(DEFAULT_STOP_JOIN_MS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        workerThread = null

        jedisPool?.close()
        jedisPool = null

        addInfo(
            "AlertumAppender stopped. sent=${sentLogs.get()} failed=${failedLogs.get()} dropped=${droppedLogs.get()}"
        )

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        if (!isStarted) return

        try {
            event.prepareForDeferredProcessing()
            val snapshot = LoggingEventVO.build(event)
            if (!queue.offer(snapshot)) {
                if (!dropWhenFull && tryEvictFor(snapshot.level) && queue.offer(snapshot)) {
                    return
                }
                droppedLogs.incrementAndGet()
            }
        } catch (_: Exception) {
            // best effort only
        }
    }

    private fun tryEvictFor(level: Level): Boolean {
        val iterator = queue.iterator()
        while (iterator.hasNext()) {
            val candidate = iterator.next()
            if (candidate.level.toInt() < level.toInt()) {
                iterator.remove()
                droppedLogs.incrementAndGet()
                return true
            }
        }
        return false
    }

    // ---------------- WORKER ----------------

    private fun workerLoop() {
        val buffer = ArrayList<ILoggingEvent>(MAX_DRAIN)

        while (running.get() || queue.isNotEmpty()) {
            try {
                val first = queue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS) ?: continue
                buffer.add(first)
                queue.drainTo(buffer, MAX_DRAIN - 1)

                if (buffer.isNotEmpty()) {
                    sendBuffer(buffer)
                    buffer.clear()
                }
            } catch (ex: InterruptedException) {
                if (!running.get()) break
                Thread.currentThread().interrupt()
            } catch (_: Exception) {
                // best effort only
            }
        }
    }

    private fun sendBuffer(events: List<ILoggingEvent>) {
        val pool = jedisPool ?: return
        try {
            pool.resource.use { jedis ->
                for (event in events) {
                    val payload = buildPayload(event)
                    jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, mapOf(PAYLOAD_FIELD to payload))
                }
            }
            sentLogs.addAndGet(events.size.toLong())
        } catch (ex: Exception) {
            failedLogs.addAndGet(events.size.toLong())
            droppedLogs.addAndGet(events.size.toLong())
            maybeWarn(ex)
        }
    }

    // ---------------- PAYLOAD ----------------

    private fun buildPayload(event: ILoggingEvent): String {
        val mdc = event.mdcPropertyMap ?: emptyMap()
        val endpoint = mdc[MDC_ENDPOINT]?.takeIf { it.isNotBlank() } ?: ""
        val statusCode = parseInt(mdc[MDC_STATUS_CODE]) ?: 0
        val durationMs = parseLong(mdc[MDC_DURATION_MS]) ?: 0L
        val traceId = mdc[MDC_TRACE_ID]?.takeIf { it.isNotBlank() }
        val spanId = mdc[MDC_SPAN_ID]?.takeIf { it.isNotBlank() }
        val explicitError = parseBoolean(mdc[MDC_ERROR])
        val error = explicitError ?: (event.level.isGreaterOrEqual(Level.ERROR) || statusCode >= 500)
        val metadata = extractMetadata(mdc)

        val record = StreamLogRecord(
            logId = UUID.randomUUID().toString(),
            timestamp = event.timeStamp,
            level = event.level.levelStr,
            message = event.formattedMessage ?: "",
            service = service,
            environment = environment,
            teamId = teamId,
            endpoint = endpoint,
            statusCode = statusCode,
            durationMs = durationMs,
            error = error,
            traceId = traceId,
            spanId = spanId,
            metadata = metadata
        )
        return objectMapper.writeValueAsString(record)
    }

    private fun extractMetadata(mdc: Map<String, String>): Map<String, String>? {
        if (mdc.isEmpty()) return null
        val result = LinkedHashMap<String, String>()
        for ((key, value) in mdc) {
            if (value.isBlank() || key in CORE_MDC_KEYS) continue
            result[key] = value
        }
        return result.takeIf { it.isNotEmpty() }
    }

    private fun parseInt(raw: String?): Int? = raw?.trim()?.toIntOrNull()

    private fun parseLong(raw: String?): Long? = raw?.trim()?.toLongOrNull()

    private fun parseBoolean(raw: String?): Boolean? = raw?.trim()?.lowercase()?.let { parseBooleanValue(it) }

    private fun parseBooleanValue(value: String): Boolean? = when (value) {
        "true", "1", "yes", "y" -> true
        "false", "0", "no", "n" -> false
        else -> null
    }

    private fun buildPoolConfig(): JedisPoolConfig {
        return JedisPoolConfig().apply {
            maxTotal = DEFAULT_POOL_MAX_TOTAL
            maxIdle = DEFAULT_POOL_MAX_IDLE
            minIdle = DEFAULT_POOL_MIN_IDLE
            testOnBorrow = true
        }
    }

    private fun maybeWarn(ex: Exception) {
        val now = System.currentTimeMillis()
        val last = lastWarnAt.get()
        if (now - last < WARN_INTERVAL_MS) return
        if (lastWarnAt.compareAndSet(last, now)) {
            addWarn("AlertumAppender Redis stream write failed: ${ex.message}")
        }
    }

    private data class StreamLogRecord(
        val logId: String,
        val timestamp: Long,
        val level: String,
        val message: String,
        val service: String,
        val environment: String,
        val teamId: String,
        val endpoint: String,
        val statusCode: Int,
        val durationMs: Long,
        val error: Boolean,
        val traceId: String?,
        val spanId: String?,
        val metadata: Map<String, String>?
    )

    companion object {
        private const val DEFAULT_QUEUE_CAPACITY = 5_000
        private const val DEFAULT_STOP_JOIN_MS = 2_000L
        private const val DEFAULT_REDIS_HOST = "localhost"
        private const val DEFAULT_REDIS_PORT = 6379
        private const val DEFAULT_STREAM_KEY = "alertum:logs"
        private const val REDIS_TIMEOUT_MS = 2_000
        private const val MAX_DRAIN = 200
        private const val POLL_INTERVAL_MS = 10L
        private const val WARN_INTERVAL_MS = 30_000L

        private const val DEFAULT_POOL_MAX_TOTAL = 8
        private const val DEFAULT_POOL_MAX_IDLE = 4
        private const val DEFAULT_POOL_MIN_IDLE = 1

        private const val PAYLOAD_FIELD = "payload"

        private const val MDC_ENDPOINT = "endpoint"
        private const val MDC_STATUS_CODE = "statusCode"
        private const val MDC_DURATION_MS = "durationMs"
        private const val MDC_TRACE_ID = "traceId"
        private const val MDC_SPAN_ID = "spanId"
        private const val MDC_ERROR = "error"

        private val CORE_MDC_KEYS = setOf(
            MDC_ENDPOINT,
            MDC_STATUS_CODE,
            MDC_DURATION_MS,
            MDC_TRACE_ID,
            MDC_SPAN_ID,
            MDC_ERROR
        )
    }
}
