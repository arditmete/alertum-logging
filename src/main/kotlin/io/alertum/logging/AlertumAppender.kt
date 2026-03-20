package io.alertum.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.core.AppenderBase
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPOutputStream

class AlertumAppender : AppenderBase<ILoggingEvent>() {

    @Volatile
    private var ingestionKey: String = ""

    @Volatile
    private var endpoint: String = DEFAULT_ENDPOINT

    @Volatile
    private var service: String = DEFAULT_SERVICE

    @Volatile
    private var environment: String = DEFAULT_ENVIRONMENT

    @Volatile
    private var batchSize: Int = DEFAULT_BATCH_SIZE

    @Volatile
    private var flushIntervalMillis: Long = DEFAULT_FLUSH_INTERVAL_MILLIS

    @Volatile
    private var queueCapacity: Int = DEFAULT_QUEUE_CAPACITY

    @Volatile
    private var requestTimeoutMillis: Long = DEFAULT_REQUEST_TIMEOUT_MILLIS

    @Volatile
    private var stopJoinMillis: Long = DEFAULT_STOP_JOIN_MILLIS

    private val running = AtomicBoolean(false)
    private val sending = AtomicBoolean(false)

    @Volatile
    private var queue: LinkedBlockingQueue<ILoggingEvent> = LinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY)

    @Volatile
    private var workerThread: Thread? = null

    private val droppedLogs = AtomicLong(0)
    private val sentBatches = AtomicLong(0)
    private val failedBatches = AtomicLong(0)

    private val httpClient: HttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build()

    fun setIngestionKey(value: String) {
        ingestionKey = value.trim()
    }

    fun setEndpoint(value: String) {
        endpoint = value.trim().ifEmpty { DEFAULT_ENDPOINT }
    }

    fun setService(value: String) {
        service = value.trim().ifEmpty { DEFAULT_SERVICE }
    }

    fun setEnvironment(value: String) {
        environment = value.trim().ifEmpty { DEFAULT_ENVIRONMENT }
    }

    fun setBatchSize(value: Int) {
        if (value > 0) {
            batchSize = value
        }
    }

    fun setFlushIntervalMillis(value: Long) {
        if (value > 0) {
            flushIntervalMillis = value
        }
    }

    fun setQueueCapacity(value: Int) {
        if (!isStarted && value > 0) {
            queueCapacity = value
        }
    }

    fun setRequestTimeoutMillis(value: Long) {
        if (value > 0) {
            requestTimeoutMillis = value
        }
    }

    fun setStopJoinMillis(value: Long) {
        if (value > 0) {
            stopJoinMillis = value
        }
    }

    override fun start() {
        if (isStarted) return

        if (ingestionKey.isBlank()) {
            addWarn("AlertumAppender ingestionKey is blank; appender will not start.")
            return
        }

        if (service.isBlank()) {
            addWarn("AlertumAppender service is blank; using default value.")
            service = DEFAULT_SERVICE
        }

        if (environment.isBlank()) {
            addWarn("AlertumAppender environment is blank; using default value.")
            environment = DEFAULT_ENVIRONMENT
        }

        queue = LinkedBlockingQueue(queueCapacity)

        if (running.compareAndSet(false, true)) {
            workerThread = Thread(::workerLoop, "alertum-log-worker").apply {
                isDaemon = true
                start()
            }
        }

        super.start()
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

        val dropped = droppedLogs.get()
        val sent = sentBatches.get()
        val failed = failedBatches.get()

        if (dropped > 0) {
            addWarn("AlertumAppender dropped $dropped log events because the queue was full.")
        }

        addInfo("AlertumAppender stopped. sentBatches=$sent failedBatches=$failed droppedLogs=$dropped")

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        if (!isStarted) return

        try {
            event.prepareForDeferredProcessing()
            val snapshot = LoggingEventVO.build(event)
            if (!queue.offer(snapshot)) {
                val dropped = droppedLogs.incrementAndGet()
                if (dropped == 1L || dropped % DROP_WARN_EVERY == 0L) {
                    addWarn("AlertumAppender queue is full; dropping log events. droppedLogs=$dropped")
                }
            }
        } catch (ex: Exception) {
            addError("Failed to enqueue log event", ex)
        }
    }

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
                    val payload = buildPayload(batch)
                    sendWithRetryAsync(payload)
                    batch.clear()
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

        if (queue.isNotEmpty()) {
            val finalBatch = ArrayList<ILoggingEvent>(batchSize)
            while (queue.isNotEmpty()) {
                queue.drainTo(finalBatch, batchSize)
                if (finalBatch.isNotEmpty()) {
                    try {
                        val payload = buildPayload(finalBatch)
                        sendWithRetrySync(payload)
                    } catch (ex: Exception) {
                        addError("AlertumAppender final flush failed", ex)
                    } finally {
                        finalBatch.clear()
                    }
                }
            }
        }
    }

    private fun buildPayload(events: List<ILoggingEvent>): String {
        val sb = StringBuilder(events.size * 256)
        sb.append('{').append("\"logs\":[")
        events.forEachIndexed { index, event ->
            if (index > 0) sb.append(',')
            sb.append(buildLogObject(event))
        }
        sb.append("]}")
        return sb.toString()
    }

    private fun buildLogObject(event: ILoggingEvent): String {
        val mdc = event.mdcPropertyMap.orEmpty()

        val traceId = mdc["traceId"]?.takeIf { it.isNotBlank() }
        val spanId = mdc["spanId"]?.takeIf { it.isNotBlank() }
        val parentSpanId = mdc["parentSpanId"]?.takeIf { it.isNotBlank() }
        val requestId = mdc["requestId"]?.takeIf { it.isNotBlank() }
        val teamId = mdc["teamId"]?.takeIf { it.isNotBlank() }
        val userId = mdc["userId"]?.takeIf { it.isNotBlank() }
        val endpointValue = mdc["endpoint"]?.takeIf { it.isNotBlank() }
        val httpMethod = mdc["httpMethod"]?.takeIf { it.isNotBlank() }
        val statusCode = mdc["statusCode"]?.toLongOrNull()
        val durationMs = mdc["durationMs"]?.toLongOrNull()
        val tags = mdc["tags"]
                ?.split(",")
                ?.map { it.trim() }
                ?.filter { it.isNotBlank() }
                .orEmpty()

        val exceptionMessage = event.throwableProxy?.message?.takeIf { it.isNotBlank() }
        val throwableClass = event.throwableProxy?.className?.takeIf { it.isNotBlank() }

        val sb = StringBuilder(256)
        sb.append('{')
        var first = true

        fun appendCommaIfNeeded() {
            if (!first) sb.append(',')
            first = false
        }

        fun appendString(name: String, value: String) {
            appendCommaIfNeeded()
            sb.append('"').append(name).append('"').append(':')
            sb.append('"').append(escape(value)).append('"')
        }

        fun appendNumber(name: String, value: Long) {
            appendCommaIfNeeded()
            sb.append('"').append(name).append('"').append(':').append(value)
        }

        fun appendOptionalString(name: String, value: String?) {
            if (value != null) appendString(name, value)
        }

        fun appendOptionalNumber(name: String, value: Long?) {
            if (value != null) appendNumber(name, value)
        }

        fun appendStringArray(name: String, values: List<String>) {
            if (values.isEmpty()) return
            appendCommaIfNeeded()
            sb.append('"').append(name).append('"').append(':').append('[')
            values.forEachIndexed { index, item ->
                if (index > 0) sb.append(',')
                sb.append('"').append(escape(item)).append('"')
            }
            sb.append(']')
        }

        appendString("message", event.formattedMessage ?: "")
        appendString("level", event.level.levelStr)
        appendString("logger", event.loggerName ?: "unknown-logger")
        appendNumber("timestamp", event.timeStamp)
        appendString("service", service)
        appendString("environment", environment)
        appendString("thread", event.threadName ?: "unknown-thread")
        appendOptionalString("traceId", traceId)
        appendOptionalString("spanId", spanId)
        appendOptionalString("parentSpanId", parentSpanId)
        appendOptionalString("requestId", requestId)
        appendOptionalString("teamId", teamId)
        appendOptionalString("userId", userId)
        appendOptionalString("endpoint", endpointValue)
        appendOptionalString("httpMethod", httpMethod)
        appendOptionalNumber("statusCode", statusCode)
        appendOptionalNumber("durationMs", durationMs)
        appendStringArray("tags", tags)

        if (exceptionMessage != null || throwableClass != null) {
            appendCommaIfNeeded()
            sb.append("\"metadata\":{")
            var metadataFirst = true

            fun appendMetadata(name: String, value: String?) {
                if (value == null) return
                if (!metadataFirst) sb.append(',')
                metadataFirst = false
                sb.append('"').append(name).append('"').append(':')
                sb.append('"').append(escape(value)).append('"')
            }

            appendMetadata("exception", exceptionMessage)
            appendMetadata("exceptionClass", throwableClass)
            sb.append('}')
        }

        sb.append('}')
        return sb.toString()
    }

    private fun sendWithRetryAsync(payload: String) {
        if (ingestionKey.isBlank()) return
        if (sending.get()) return

        val request = buildRequest(payload)
        sending.set(true)

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .whenComplete { response, error ->
                    try {
                        if (error != null) {
                            retryOnceAsync(payload, "request failed", error)
                            return@whenComplete
                        }

                        val status = response.statusCode()
                        if (status in 200..299) {
                            sentBatches.incrementAndGet()
                            return@whenComplete
                        }

                        if (status >= 500) {
                            retryOnceAsync(payload, "status=$status", null)
                        } else {
                            failedBatches.incrementAndGet()
                            addError("AlertumAppender failed to send logs (status=$status)")
                        }
                    } finally {
                        sending.set(false)
                    }
                }
    }

    private fun retryOnceAsync(payload: String, reason: String, error: Throwable?) {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }

        val request = buildRequest(payload)

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .whenComplete { response, retryError ->
                    if (retryError != null) {
                        failedBatches.incrementAndGet()
                        addError("AlertumAppender retry failed ($reason)", error ?: retryError as? Exception ?: RuntimeException(retryError))
                        return@whenComplete
                    }

                    val retryStatus = response.statusCode()
                    if (retryStatus in 200..299) {
                        sentBatches.incrementAndGet()
                    } else {
                        failedBatches.incrementAndGet()
                        addError("AlertumAppender retry failed (status=$retryStatus)")
                    }
                }
    }

    private fun sendWithRetrySync(payload: String) {
        if (ingestionKey.isBlank()) return

        val request = buildRequest(payload)

        try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
            val status = response.statusCode()
            if (status in 200..299) {
                sentBatches.incrementAndGet()
                return
            }

            if (status >= 500) {
                Thread.sleep(RETRY_BACKOFF_MILLIS)
                val retryResponse = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
                if (retryResponse.statusCode() in 200..299) {
                    sentBatches.incrementAndGet()
                } else {
                    failedBatches.incrementAndGet()
                    addError("AlertumAppender final flush failed (status=${retryResponse.statusCode()})")
                }
            } else {
                failedBatches.incrementAndGet()
                addError("AlertumAppender final flush failed (status=$status)")
            }
        } catch (ex: Exception) {
            failedBatches.incrementAndGet()
            addError("AlertumAppender final flush request failed", ex)
        }
    }

    private fun buildRequest(payload: String): HttpRequest {
        val compressedPayload = gzip(payload)
        return HttpRequest.newBuilder()
                .uri(ingestUri())
                .timeout(Duration.ofMillis(requestTimeoutMillis))
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .header("Accept", "application/json")
                .header("X-API-Key", ingestionKey)
                .POST(HttpRequest.BodyPublishers.ofByteArray(compressedPayload))
                .build()
    }

    private fun ingestUri(): URI {
        val base = endpoint.trim().removeSuffix("/")
        return URI.create("$base/api/logs/ingest")
    }

    private fun gzip(value: String): ByteArray {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        val outputStream = ByteArrayOutputStream(bytes.size)
        GZIPOutputStream(outputStream).use { gzip ->
            gzip.write(bytes)
        }
        return outputStream.toByteArray()
    }

    private fun escape(value: String): String {
        val sb = StringBuilder(value.length + 16)
        for (ch in value) {
            when (ch) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\b' -> sb.append("\\b")
                '\u000C' -> sb.append("\\f")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> {
                    if (ch.code < 32) {
                        sb.append("\\u")
                        sb.append(ch.code.toString(16).padStart(4, '0'))
                    } else {
                        sb.append(ch)
                    }
                }
            }
        }
        return sb.toString()
    }

    companion object {
        private const val DEFAULT_ENDPOINT = "https://api.alertum.co"
        private const val DEFAULT_SERVICE = "unknown-service"
        private const val DEFAULT_ENVIRONMENT = ""

        private const val DEFAULT_BATCH_SIZE = 50
        private const val DEFAULT_FLUSH_INTERVAL_MILLIS = 1000L
        private const val DEFAULT_QUEUE_CAPACITY = 2000
        private const val DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000L
        private const val DEFAULT_STOP_JOIN_MILLIS = 5000L
        private const val RETRY_BACKOFF_MILLIS = 150L
        private const val DROP_WARN_EVERY = 100L
    }
}