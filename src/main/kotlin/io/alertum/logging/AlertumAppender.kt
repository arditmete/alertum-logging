package io.alertum.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.classic.spi.ThrowableProxyUtil
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

    private val droppedLogs = AtomicLong(0)
    private val sentBatches = AtomicLong(0)
    private val failedBatches = AtomicLong(0)

    private val httpClient: HttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build()

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

    // ---------------- LIFECYCLE ----------------
    override fun start() {
        if (isStarted) return

        if (ingestionKey.isBlank()) {
            addWarn("AlertumAppender started without ingestionKey — will wait for config")
        }
        if (endpoint.contains("\${")) {
            addWarn("Endpoint not resolved yet — waiting for config")
        }

        addInfo("AlertumAppender start called. endpoint=$endpoint service=$service environment=$environment")

        queue = LinkedBlockingQueue(queueCapacity)

        if (running.compareAndSet(false, true)) {
            workerThread = Thread(::workerLoop, "alertum-log-worker").apply {
                isDaemon = true
                start()
            }
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
        }.start()

        super.start()
    }

    private fun sendAsync(payload: String) {
        if (ingestionKey.isBlank()) return
        if (!endpoint.startsWith("http")) return
        if (endpoint.contains("\${")) return

        val request = buildRequest(payload)
        addInfo("AlertumAppender sending batch of ${payload.length} chars to ${ingestUri()}")

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .whenComplete { response, error ->
                    if (error != null) {
                        failedBatches.incrementAndGet()
                        addWarn("Send failed: ${error.message}")
                        return@whenComplete
                    }

                    val status = response.statusCode()
                    if (status in 200..299) {
                        sentBatches.incrementAndGet()
                        addInfo("AlertumAppender batch sent successfully")
                    } else {
                        failedBatches.incrementAndGet()
                        addWarn("Send failed status=$status")
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
        workerThread?.join(stopJoinMillis)
        workerThread = null

        addInfo("AlertumAppender stopped. sent=${sentBatches.get()} failed=${failedBatches.get()} dropped=${droppedLogs.get()}")

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        if (!isStarted) return

        event.prepareForDeferredProcessing()
        val snapshot = LoggingEventVO.build(event)

        if (!queue.offer(snapshot)) {
            val dropped = droppedLogs.incrementAndGet()
            if (dropped % 100 == 0L) {
                addWarn("Dropped logs=$dropped")
            }
        }
    }

    // ---------------- WORKER ----------------

    private fun workerLoop() {
        val batch = ArrayList<ILoggingEvent>(batchSize)

        while (running.get() || queue.isNotEmpty()) {
            val first = queue.poll(flushIntervalMillis, TimeUnit.MILLISECONDS)
            if (first != null) {
                batch.add(first)
                queue.drainTo(batch, batchSize - 1)
            }

            if (batch.isNotEmpty()) {
                val payload = buildPayload(batch)
                sendAsync(payload)
                batch.clear()
            }
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
            addInfo("Alertum connection OK")
        } else {
            addWarn("Alertum connection failed status=${response.statusCode()}")
        }
    }

    // ---------------- PAYLOAD ----------------

    private fun buildPayload(events: List<ILoggingEvent>): String {
        val sb = StringBuilder()
        sb.append("{\"logs\":[")
        events.forEachIndexed { i, e ->
            if (i > 0) sb.append(',')
            val mdc = e.mdcPropertyMap
            val traceId = mdc?.get("traceId")
            val spanId = mdc?.get("spanId")
            val requestId = mdc?.get("requestId")
            val endpoint = mdc?.get("endpoint")
            val httpMethod = mdc?.get("httpMethod")
            val statusCode = mdc?.get("statusCode")?.toIntOrNull()
                ?.takeIf { it in 100..599 }
            val rawDurationMs = mdc?.get("durationMs")?.toLongOrNull()
            val requestStartMs = mdc?.get("requestStartMs")?.toLongOrNull()
            val durationMs = when {
                rawDurationMs != null && rawDurationMs > 0 -> rawDurationMs
                requestStartMs != null -> (e.timeStamp - requestStartMs).coerceAtLeast(0)
                else -> null
            }
            val throwable = e.throwableProxy
            val exceptionClass = throwable?.className
            val exceptionMessage = throwable?.message
            val stacktrace = throwable?.let { ThrowableProxyUtil.asString(it) }
                ?.let { truncate(it, MAX_STACKTRACE_CHARS) }
            sb.append("{")
            sb.append("\"message\":\"").append(escape(e.formattedMessage ?: "")).append("\",")
            sb.append("\"level\":\"").append(escape(e.level.levelStr)).append("\",")
            sb.append("\"timestamp\":").append(e.timeStamp).append(",")
            sb.append("\"logger\":\"").append(escape(e.loggerName ?: "")).append("\",")
            sb.append("\"thread\":\"").append(escape(e.threadName ?: "")).append("\",")
            sb.append("\"service\":\"").append(escape(service)).append("\",")
            sb.append("\"environment\":\"").append(escape(environment)).append("\"")
            if (!traceId.isNullOrBlank()) {
                sb.append(",\"traceId\":\"").append(escape(traceId)).append("\"")
            }
            if (!spanId.isNullOrBlank()) {
                sb.append(",\"spanId\":\"").append(escape(spanId)).append("\"")
            }
            if (!requestId.isNullOrBlank()) {
                sb.append(",\"requestId\":\"").append(escape(requestId)).append("\"")
            }
            if (!endpoint.isNullOrBlank()) {
                sb.append(",\"endpoint\":\"").append(escape(endpoint)).append("\"")
            }
            if (!httpMethod.isNullOrBlank()) {
                sb.append(",\"httpMethod\":\"").append(escape(httpMethod)).append("\"")
            }
            if (statusCode != null) {
                sb.append(",\"statusCode\":").append(statusCode)
            }
            if (durationMs != null) {
                sb.append(",\"durationMs\":").append(durationMs)
            }
            if (exceptionClass != null) {
                sb.append(",\"metadata\":{")
                sb.append("\"exceptionClass\":\"").append(escape(exceptionClass)).append("\"")
                if (!exceptionMessage.isNullOrBlank()) {
                    sb.append(",\"exceptionMessage\":\"").append(escape(exceptionMessage)).append("\"")
                }
                if (!stacktrace.isNullOrBlank()) {
                    sb.append(",\"stacktrace\":\"").append(escape(stacktrace)).append("\"")
                }
                sb.append("}")
            }
            sb.append("}")
        }
        sb.append("]}")
        return sb.toString()
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

    private fun escape(value: String): String {
        val sb = StringBuilder(value.length + 16)
        for (c in value) {
            when (c) {
                '\\' -> sb.append("\\\\")
                '"' -> sb.append("\\\"")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> if (c < ' ') sb.append("\\u%04x".format(c.code)) else sb.append(c)
            }
        }
        return sb.toString()
    }

    private fun truncate(value: String, maxLen: Int): String {
        return if (value.length <= maxLen) value else value.substring(0, maxLen)
    }

    companion object {
        private const val MAX_STACKTRACE_CHARS = 12_000
    }
}
