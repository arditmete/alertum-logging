package io.alertum.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.core.AppenderBase
import io.alertum.logs.v1.LogEntry
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class AlertumAppender : AppenderBase<ILoggingEvent>() {

    @Volatile private var teamId: String = System.getenv("ALERTUM_TEAM_ID")?.trim().orEmpty()
    @Volatile private var apiKey: String? = System.getenv("ALERTUM_API_KEY")?.trim()?.takeIf { it.isNotEmpty() }
    @Volatile private var service: String = AlertumDefaults.DEFAULT_SERVICE
    @Volatile private var environment: String = AlertumDefaults.DEFAULT_ENVIRONMENT

    @Volatile private var ingestionHost: String =
        System.getenv("ALERTUM_INGESTION_HOST")?.trim()?.takeIf { it.isNotEmpty() } ?: DEFAULT_INGESTION_HOST
    @Volatile private var ingestionPort: Int =
        System.getenv("ALERTUM_INGESTION_PORT")?.toIntOrNull()?.takeIf { it > 0 } ?: DEFAULT_INGESTION_PORT

    @Volatile private var batchSize: Int =
        System.getenv("ALERTUM_BATCH_SIZE")?.toIntOrNull()?.takeIf { it > 0 } ?: DEFAULT_BATCH_SIZE
    @Volatile private var flushIntervalMs: Long =
        System.getenv("ALERTUM_FLUSH_INTERVAL_MS")?.toLongOrNull()?.takeIf { it > 0 } ?: DEFAULT_FLUSH_INTERVAL_MS
    @Volatile private var maxQueueSize: Int =
        System.getenv("ALERTUM_MAX_QUEUE_SIZE")?.toIntOrNull()?.takeIf { it > 0 }
            ?: System.getenv("ALERTUM_QUEUE_CAPACITY")?.toIntOrNull()?.takeIf { it > 0 }
            ?: DEFAULT_MAX_QUEUE_SIZE

    private val running = AtomicBoolean(false)

    @Volatile
    private var queue = LinkedBlockingQueue<ILoggingEvent>(maxQueueSize)

    @Volatile
    private var workerThread: Thread? = null

    @Volatile
    private var transport: AlertumGrpcTransport? = null

    private val logsQueued = AtomicLong(0)
    private val logsSent = AtomicLong(0)
    private val logsDropped = AtomicLong(0)
    private val batchesSent = AtomicLong(0)
    private val retries = AtomicLong(0)
    private val inFlightBatches = AtomicInteger(0)
    private val lastOverflowWarnAt = AtomicLong(0)
    private val lastFailureWarnAt = AtomicLong(0)

    // ---------------- SETTERS ----------------

    fun setTeamId(value: String) {
        if (isStarted) return
        teamId = value.trim()
    }

    fun setApiKey(value: String) {
        if (isStarted) return
        apiKey = value.trim().ifEmpty { null }
    }

    fun setService(value: String) {
        service = value.trim().ifEmpty { AlertumDefaults.DEFAULT_SERVICE }
    }

    fun setEnvironment(value: String) {
        environment = value.trim().ifBlank { AlertumDefaults.DEFAULT_ENVIRONMENT }
    }

    fun setIngestionHost(value: String) {
        if (isStarted) return
        ingestionHost = value.trim().ifEmpty { DEFAULT_INGESTION_HOST }
    }

    fun setIngestionPort(value: Int) {
        if (isStarted) return
        if (value > 0) ingestionPort = value
    }

    fun setBatchSize(value: Int) {
        if (isStarted) return
        if (value > 0) batchSize = value
    }

    fun setFlushIntervalMs(value: Long) {
        if (isStarted) return
        if (value > 0) flushIntervalMs = value
    }

    fun setMaxQueueSize(value: Int) {
        if (isStarted) return
        if (value > 0) maxQueueSize = value
    }

    fun getLogsQueued(): Long = logsQueued.get()

    fun getLogsSent(): Long = logsSent.get()

    fun getLogsDropped(): Long = logsDropped.get()

    fun getBatchesSent(): Long = batchesSent.get()

    fun getRetries(): Long = retries.get()

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

        queue = LinkedBlockingQueue(maxQueueSize)

        transport = AlertumGrpcTransport(
            host = ingestionHost,
            port = ingestionPort,
            apiKey = apiKey,
            sdkVersion = SDK_VERSION,
            source = SDK_SOURCE,
            onSuccess = { count ->
                logsSent.addAndGet(count.toLong())
                batchesSent.incrementAndGet()
                inFlightBatches.decrementAndGet()
            },
            onFailure = { count, _ ->
                logsDropped.addAndGet(count.toLong())
                inFlightBatches.decrementAndGet()
                maybeWarnFailure()
            },
            onRetry = { retries.incrementAndGet() }
        )

        super.start()

        if (running.compareAndSet(false, true)) {
            workerThread = Thread(::workerLoop, "alertum-grpc-worker").apply {
                isDaemon = true
                start()
            }
        }

        addInfo("AlertumAppender started (grpc) host=$ingestionHost port=$ingestionPort")
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

        flushRemaining(DEFAULT_STOP_JOIN_MS)
        awaitInFlight(DEFAULT_STOP_JOIN_MS)

        transport?.shutdown(DEFAULT_STOP_JOIN_MS)
        transport = null

        addInfo(
            "AlertumAppender stopped. queued=${logsQueued.get()} sent=${logsSent.get()} dropped=${logsDropped.get()} batches=${batchesSent.get()} retries=${retries.get()}"
        )

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        if (!isStarted) return

        try {
            event.prepareForDeferredProcessing()
            val snapshot = LoggingEventVO.build(event)
            if (!queue.offer(snapshot)) {
                logsDropped.incrementAndGet()
                maybeWarnOverflow()
            } else {
                logsQueued.incrementAndGet()
            }
        } catch (_: Exception) {
            // best effort only
        }
    }

    // ---------------- WORKER ----------------

    private fun workerLoop() {
        val batch = ArrayList<ILoggingEvent>(batchSize)

        while (running.get() || queue.isNotEmpty()) {
            try {
                val first = queue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS) ?: continue
                batch.add(first)

                val deadline = System.currentTimeMillis() + flushIntervalMs
                while (batch.size < batchSize) {
                    val remaining = deadline - System.currentTimeMillis()
                    if (remaining <= 0L) break

                    val next = queue.poll(minOf(POLL_INTERVAL_MS, remaining), TimeUnit.MILLISECONDS)
                    if (next != null) {
                        batch.add(next)
                    } else {
                        break
                    }
                }

                if (batch.isNotEmpty()) {
                    sendBatch(batch)
                    batch.clear()
                }
            } catch (ex: InterruptedException) {
                if (!running.get()) break
                Thread.currentThread().interrupt()
            } catch (_: Exception) {
                // best effort only
            }
        }
    }

    private fun sendBatch(events: List<ILoggingEvent>) {
        val grpcBatch = ArrayList<LogEntry>(events.size)
        for (event in events) {
            grpcBatch.add(mapToGrpc(event))
        }
        if (grpcBatch.isEmpty()) return
        inFlightBatches.incrementAndGet()
        transport?.send(grpcBatch)
    }

    private fun mapToGrpc(event: ILoggingEvent): LogEntry {
        val mdc = event.mdcPropertyMap ?: emptyMap()
        val endpoint = mdc[MDC_ENDPOINT]?.takeIf { it.isNotBlank() } ?: ""
        val statusCode = parseInt(mdc[MDC_STATUS_CODE]) ?: 0
        val durationMs = parseLong(mdc[MDC_DURATION_MS]) ?: 0L
        val traceId = mdc[MDC_TRACE_ID]?.takeIf { it.isNotBlank() }
        val spanId = mdc[MDC_SPAN_ID]?.takeIf { it.isNotBlank() }
        val explicitError = parseBoolean(mdc[MDC_ERROR])
        val error = explicitError ?: (event.level.isGreaterOrEqual(Level.ERROR) || statusCode >= 500)
        val metadata = extractMetadata(mdc)

        val builder = LogEntry.newBuilder()
            .setLogId(UUID.randomUUID().toString())
            .setTimestampUnixMs(event.timeStamp)
            .setLevel(event.level.levelStr)
            .setMessage(event.formattedMessage ?: "")
            .setService(service)
            .setEnvironment(environment)
            .setTeamId(teamId)
            .setEndpoint(endpoint)
            .setStatusCode(statusCode)
            .setDurationMs(durationMs)
            .setError(error)

        if (traceId != null) builder.setTraceId(traceId)
        if (spanId != null) builder.setSpanId(spanId)
        if (metadata != null) builder.putAllMetadata(metadata)

        return builder.build()
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

    private fun maybeWarnOverflow() {
        val now = System.currentTimeMillis()
        val last = lastOverflowWarnAt.get()
        if (now - last < WARN_INTERVAL_MS) return
        if (lastOverflowWarnAt.compareAndSet(last, now)) {
            addWarn("AlertumAppender queue full. Dropping logs. queueSize=${queue.size} maxQueueSize=$maxQueueSize")
        }
    }

    private fun maybeWarnFailure() {
        val now = System.currentTimeMillis()
        val last = lastFailureWarnAt.get()
        if (now - last < WARN_INTERVAL_MS) return
        if (lastFailureWarnAt.compareAndSet(last, now)) {
            addError("AlertumAppender gRPC send failed. Dropping batch.")
        }
    }

    private fun flushRemaining(timeoutMs: Long) {
        val deadline = System.currentTimeMillis() + timeoutMs
        val batch = ArrayList<ILoggingEvent>(batchSize)

        while (System.currentTimeMillis() < deadline && queue.isNotEmpty()) {
            queue.drainTo(batch, batchSize)
            if (batch.isNotEmpty()) {
                sendBatch(batch)
                batch.clear()
            } else {
                break
            }
        }
    }

    private fun awaitInFlight(timeoutMs: Long) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (inFlightBatches.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(INFLIGHT_POLL_MS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }
    }

    companion object {
        private const val DEFAULT_BATCH_SIZE = 50
        private const val DEFAULT_FLUSH_INTERVAL_MS = 2_000L
        private const val DEFAULT_MAX_QUEUE_SIZE = 10_000
        private const val DEFAULT_STOP_JOIN_MS = 2_000L
        private const val DEFAULT_INGESTION_HOST = "localhost"
        private const val DEFAULT_INGESTION_PORT = 9090
        private const val POLL_INTERVAL_MS = 25L
        private const val WARN_INTERVAL_MS = 30_000L
        private const val INFLIGHT_POLL_MS = 25L
        private const val SDK_VERSION = "alertum-logging"
        private const val SDK_SOURCE = "logback"

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
