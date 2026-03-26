package io.alertum.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPOutputStream

class AlertumAppender : AppenderBase<ILoggingEvent>() {

    @Volatile private var ingestionKey: String = ""
    @Volatile private var endpoint: String = AlertumDefaults.DEFAULT_ENDPOINT
    @Volatile private var service: String = AlertumDefaults.DEFAULT_SERVICE
    @Volatile private var environment: String = AlertumDefaults.DEFAULT_ENVIRONMENT

    @Volatile private var batchSize: Int = 50
    @Volatile private var flushIntervalMillis: Long = 1000
    @Volatile private var queueCapacity: Int = 2000
    @Volatile private var requestTimeoutMillis: Long = 3000
    @Volatile private var stopJoinMillis: Long = 5000
    @Volatile private var gzipEnabled: Boolean = false

    private val running = AtomicBoolean(false)

    @Volatile
    private var queue = LinkedBlockingQueue<ILoggingEvent>(queueCapacity)

    @Volatile
    private var workerThread: Thread? = null

    @Volatile
    private var retryScheduler: ScheduledExecutorService? = null

    private val droppedLogs = AtomicLong(0)
    private val sentBatches = AtomicLong(0)
    private val failedBatches = AtomicLong(0)

    private val httpClient: HttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build()

    private val pipelineDebugEnabled = java.lang.Boolean.getBoolean("alertum.pipeline.debug")

    private val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    // ---------------- SETTERS ----------------

    fun setIngestionKey(value: String) {
        ingestionKey = value.trim()
    }

    fun setEndpoint(value: String) {
        endpoint = value.trim().ifEmpty { AlertumDefaults.DEFAULT_ENDPOINT }
    }

    fun setService(value: String) {
        service = value.trim().ifEmpty { AlertumDefaults.DEFAULT_SERVICE }
    }

    fun setEnvironment(value: String) {
        environment = value.trim().ifBlank { AlertumDefaults.DEFAULT_ENVIRONMENT }
    }

    fun setBatchSize(value: Int) {
        if (value > 0) batchSize = value
    }

    fun setFlushIntervalMillis(value: Long) {
        if (value > 0) flushIntervalMillis = value
    }

    fun setQueueCapacity(value: Int) {
        if (!isStarted && value > 0) queueCapacity = value
    }

    fun setRequestTimeoutMillis(value: Long) {
        if (value > 0) requestTimeoutMillis = value
    }

    fun setStopJoinMillis(value: Long) {
        if (value > 0) stopJoinMillis = value
    }

    fun setGzipEnabled(value: Boolean) {
        gzipEnabled = value
    }

    fun getDroppedLogs(): Long = droppedLogs.get()

    fun getSentBatches(): Long = sentBatches.get()

    fun getFailedBatches(): Long = failedBatches.get()

    fun getQueueSize(): Int = queue.size

    // ---------------- LIFECYCLE ----------------
    override fun start() {
        if (isStarted) return

        println("ALERTUM APPENDER STARTING")
        super.start()

        try {
            if (ingestionKey.isBlank()) {
                addWarn("AlertumAppender started without ingestionKey — will wait for config")
            }
            addWarn("INGESTION KEY = [$ingestionKey]")
            if (endpoint.contains("\${")) {
                addWarn("Endpoint not resolved yet — waiting for config")
            }

            pipelineDebug("AlertumAppender start called. endpoint=$endpoint service=$service environment=$environment")

            queue = LinkedBlockingQueue(queueCapacity)
            addWarn("QUEUE SIZE = ${queue.size}")
            pipelineDebug("QUEUE SIZE = ${queue.size}")
            ensureRetryScheduler()

            if (running.compareAndSet(false, true)) {
                workerThread = Thread(::workerLoop, "alertum-log-worker").apply {
                    isDaemon = true
                    start()
                }
                println("WORKER STARTED")
                pipelineDebug("Worker thread started: ${workerThread?.isAlive}")
            }

            Thread {
                try {
                    if (ingestionKey.isBlank()) return@Thread
                    if (!endpoint.startsWith("http")) return@Thread
                    if (endpoint.contains("\${")) return@Thread
                    testConnection()
                } catch (ex: Exception) {
                    addWarn("Alertum not reachable at startup: ${ex.message}")
                }
            }.apply { isDaemon = true }.start()
        } catch (ex: Exception) {
            addError("AlertumAppender failed to initialize", ex)
        }
    }

    private fun sendAsync(
        payload: String,
        events: List<ILoggingEvent>,
        attempt: Int,
        requeueAttempted: Boolean
    ) {
        if (!running.get()) return
        if (ingestionKey.isBlank()) {
            println("SEND SKIPPED: ingestionKey is blank")
            return
        }
        if (!endpoint.startsWith("http")) {
            println("SEND SKIPPED: endpoint is not http ($endpoint)")
            return
        }
        if (endpoint.contains("\${")) {
            println("SEND SKIPPED: endpoint not resolved ($endpoint)")
            return
        }

        val request = try {
            buildRequest(payload)
        } catch (ex: Exception) {
            failedBatches.incrementAndGet()
            println("SEND FAILED: request build error")
            ex.printStackTrace()
            addError("AlertumAppender failed to build request", ex)
            return
        }

        if (attempt == 0) {
            pipelineDebug("AlertumAppender sending batch of ${payload.length} chars to ${ingestUri()}")
        }

        println("SENDING LOG TO: ${request.uri()}")
        println("FINAL JSON PAYLOAD: $payload")

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, error ->
                    try {
                        if (error != null) {
                            println("SEND FAILED: ${error.message}")
                            error.printStackTrace()
                            handleRetryableFailure(payload, events, attempt, requeueAttempted, error)
                            return@whenComplete
                        }

                        val status = response.statusCode()
                        println("RESPONSE STATUS: $status")
                        if (status in 200..299) {
                            sentBatches.incrementAndGet()
                            return@whenComplete
                        }

                        if (status >= 500) {
                            val body = response.body()
                            if (!body.isNullOrBlank()) {
                                println("RESPONSE BODY: $body")
                            }
                            handleRetryableFailure(payload, events, attempt, requeueAttempted, null)
                            return@whenComplete
                        }

                        failedBatches.incrementAndGet()
                        addWarn("AlertumAppender send failed status=$status")
                        val body = response.body()
                        if (!body.isNullOrBlank()) {
                            println("RESPONSE BODY: $body")
                        }
                        if (!requeueAttempted) {
                            tryRequeue(events)
                        }
                    } catch (ex: Exception) {
                        failedBatches.incrementAndGet()
                        println("SEND FAILED: ${ex.message}")
                        ex.printStackTrace()
                        addError("AlertumAppender send failed", ex)
                        if (!requeueAttempted) {
                            tryRequeue(events)
                        }
                    }
                }
    }

    private fun ingestUri(): URI {
        val base = endpoint.trim().removeSuffix("/")
        return when {
            base.endsWith("/api/logs/ingest") -> URI.create(base)
            base.endsWith("/api/logs") -> URI.create("$base/ingest")
            else -> URI.create("$base/api/logs/ingest")
        }
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) {
            super.stop()
            return
        }

        workerThread?.interrupt()
        try {
            workerThread?.join(stopJoinMillis)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        workerThread = null

        retryScheduler?.shutdownNow()
        retryScheduler = null

        addWarn("AlertumAppender stopped. sent=${sentBatches.get()} failed=${failedBatches.get()} dropped=${droppedLogs.get()}")

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        println("APPENDER RECEIVED: ${event.formattedMessage}")
        addWarn("APPENDER RECEIVED: level=${event.level} logger=${event.loggerName} msg=${event.formattedMessage}")
        if (!isStarted) return

        try {
            pipelineDebug("Appender received event: ${event.formattedMessage ?: ""}")
            event.prepareForDeferredProcessing()
            val snapshot = LoggingEventVO.build(event)
            if (!enqueueEvent(snapshot)) {
                val dropped = droppedLogs.incrementAndGet()
                if (dropped == 1L || dropped % DROP_WARN_EVERY == 0L) {
                    addWarn("Dropped logs=$dropped")
                }
            }
        } catch (ex: Exception) {
            addError("Failed to enqueue log event", ex)
        }
    }

    private fun enqueueEvent(event: ILoggingEvent): Boolean {
        if (queue.offer(event)) return true
        if (pipelineDebugEnabled && queue.remainingCapacity() == 0) {
            addError("Queue is full")
        }

        val level = event.level
        if (level.isGreaterOrEqual(Level.WARN)) {
            if (evictLowerPriority()) {
                return queue.offer(event)
            }
        }

        return false
    }

    private fun evictLowerPriority(): Boolean {
        if (evictFirstMatching { it.level == Level.TRACE || it.level == Level.DEBUG }) {
            return true
        }
        return evictFirstMatching { it.level == Level.INFO }
    }

    private fun evictFirstMatching(match: (ILoggingEvent) -> Boolean): Boolean {
        val iterator = queue.iterator()
        while (iterator.hasNext()) {
            val candidate = iterator.next()
            if (match(candidate)) {
                iterator.remove()
                droppedLogs.incrementAndGet()
                return true
            }
        }
        return false
    }

    // ---------------- WORKER ----------------

    private fun workerLoop() {
        val batch = ArrayList<ILoggingEvent>(batchSize)

        while (running.get() || queue.isNotEmpty()) {
            try {
                val first = queue.poll(flushIntervalMillis, TimeUnit.MILLISECONDS)
                if (first != null) {
                    batch.add(first)
                    queue.drainTo(batch, batchSize - 1)
                }

                if (batch.isNotEmpty()) {
                    val snapshot = ArrayList(batch)
                    val requeueAttempted = snapshot.any { it is RequeuedLoggingEvent }
                    try {
                        for (event in snapshot) {
                            println("DEQUEUED EVENT: ${event.formattedMessage}")
                        }
                        pipelineDebug("Sending batch size=${snapshot.size} queueSize=${queue.size}")
                        val payload = buildPayload(snapshot)
                        sendAsync(payload, snapshot, 0, requeueAttempted)
                    } catch (ex: Exception) {
                        failedBatches.incrementAndGet()
                        droppedLogs.addAndGet(snapshot.size.toLong())
                        addError("AlertumAppender failed to build payload", ex)
                    } finally {
                        batch.clear()
                    }
                }
            } catch (ex: InterruptedException) {
                if (!running.get()) {
                    break
                }
                Thread.currentThread().interrupt()
            } catch (ex: Exception) {
                addError("AlertumAppender worker failure", ex)
            }
        }
    }

    private fun handleRetryableFailure(
        payload: String,
        events: List<ILoggingEvent>,
        attempt: Int,
        requeueAttempted: Boolean,
        error: Throwable?
    ) {
        if (attempt < MAX_RETRIES) {
            scheduleRetry(payload, events, attempt + 1, requeueAttempted)
            return
        }

        failedBatches.incrementAndGet()
        addWarn("AlertumAppender send failed after retries${error?.message?.let { ": $it" } ?: ""}")
        if (!requeueAttempted) {
            tryRequeue(events)
        }
    }

    private fun scheduleRetry(
        payload: String,
        events: List<ILoggingEvent>,
        attempt: Int,
        requeueAttempted: Boolean
    ) {
        val scheduler = retryScheduler
        if (scheduler == null || scheduler.isShutdown) {
            failedBatches.incrementAndGet()
            if (!requeueAttempted) {
                tryRequeue(events)
            }
            return
        }

        val delay = RETRY_BACKOFF_MS[(attempt - 1).coerceIn(0, RETRY_BACKOFF_MS.lastIndex)]
        try {
            scheduler.schedule(
                { sendAsync(payload, events, attempt, requeueAttempted) },
                delay,
                TimeUnit.MILLISECONDS
            )
        } catch (_: RejectedExecutionException) {
            failedBatches.incrementAndGet()
            if (!requeueAttempted) {
                tryRequeue(events)
            }
        }
    }

    private fun tryRequeue(events: List<ILoggingEvent>) {
        val queueRef = queue
        if (queueRef.remainingCapacity() < events.size) {
            droppedLogs.addAndGet(events.size.toLong())
            return
        }

        var enqueued = 0
        for (event in events) {
            val wrapped = if (event is RequeuedLoggingEvent) event else RequeuedLoggingEvent(event)
            if (queueRef.offer(wrapped)) {
                enqueued++
            } else {
                break
            }
        }

        val dropped = events.size - enqueued
        if (dropped > 0) {
            droppedLogs.addAndGet(dropped.toLong())
        }
    }

    private fun ensureRetryScheduler() {
        if (retryScheduler != null && retryScheduler?.isShutdown == false) return
        retryScheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "alertum-log-retry").apply { isDaemon = true }
        }
    }

    private fun pipelineDebug(message: String) {
        if (pipelineDebugEnabled) {
            addWarn(message)
        }
    }

    private fun testConnection() {
        val request = HttpRequest.newBuilder()
                .uri(ingestUri())
                .timeout(Duration.ofSeconds(2))
                .header("X-API-Key", ingestionKey)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())

        if (response.statusCode() in 200..299) {
            addWarn("Alertum connection OK")
        } else {
            addWarn("Alertum connection failed status=${response.statusCode()}")
        }
    }

    // ---------------- PAYLOAD ----------------

    private fun buildPayload(events: List<ILoggingEvent>): String {
        val entries = events.map { event ->
            val mdc = event.mdcPropertyMap.orEmpty()
            val serviceValue = mdc["service"]?.takeIf { it.isNotBlank() } ?: service
            val environmentValue = mdc["environment"]?.takeIf { it.isNotBlank() } ?: environment

            LogEntry(
                message = event.formattedMessage ?: "",
                level = event.level.levelStr,
                timestamp = event.timeStamp,
                logger = event.loggerName?.takeIf { it.isNotBlank() },
                thread = event.threadName?.takeIf { it.isNotBlank() },
                service = serviceValue.takeIf { it.isNotBlank() },
                environment = environmentValue.takeIf { it.isNotBlank() }
            )
        }

        val payload = LogPayload(entries)
        return objectMapper.writeValueAsString(payload)
    }

    private fun buildRequest(payload: String): HttpRequest {
        val builder = HttpRequest.newBuilder()
                .uri(ingestUri())
                .timeout(Duration.ofMillis(requestTimeoutMillis))
                .header("Content-Type", "application/json")
                .header("X-API-Key", ingestionKey)

        if (gzipEnabled) {
            builder.header("Content-Encoding", "gzip")
            builder.POST(HttpRequest.BodyPublishers.ofByteArray(gzip(payload)))
        } else {
            builder.POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
        }

        return builder.build()
    }

    private fun gzip(value: String): ByteArray {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        val out = ByteArrayOutputStream()
        GZIPOutputStream(out).use { it.write(bytes) }
        return out.toByteArray()
    }

    private class RequeuedLoggingEvent(
        private val delegate: ILoggingEvent
    ) : ILoggingEvent by delegate

    companion object {
        private const val MAX_RETRIES = 3
        private val RETRY_BACKOFF_MS = longArrayOf(200L, 500L, 1_000L)
        private const val DROP_WARN_EVERY = 100L
    }

    private data class LogPayload(
        val logs: List<LogEntry>
    )

    private data class LogEntry(
        val message: String,
        val level: String,
        val timestamp: Long,
        val logger: String?,
        val thread: String?,
        val service: String?,
        val environment: String?
    )
}
