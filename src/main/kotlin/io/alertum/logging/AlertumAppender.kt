package io.alertum.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEventVO
import ch.qos.logback.core.AppenderBase
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class AlertumAppender : AppenderBase<ILoggingEvent>() {

    @Volatile
    private var ingestionKey: String = ""

    @Volatile
    private var endpoint: String = DEFAULT_ENDPOINT

    @Volatile
    private var service: String = DEFAULT_SERVICE

    @Volatile
    private var environment: String = DEFAULT_ENVIRONMENT

    private val running = AtomicBoolean(false)
    private val queue = LinkedBlockingQueue<ILoggingEvent>(QUEUE_CAPACITY)
    private var workerThread: Thread? = null

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

    override fun start() {
        if (ingestionKey.isBlank()) {
            addWarn("AlertumAppender ingestionKey is blank; appender will not start.")
            return
        }

        if (running.compareAndSet(false, true)) {
            workerThread = Thread({ workerLoop() }, "alertum-log-worker").apply {
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
        workerThread?.join(STOP_JOIN_MILLIS)
        workerThread = null

        super.stop()
    }

    override fun append(event: ILoggingEvent) {
        if (!isStarted) return

        try {
            event.prepareForDeferredProcessing()
            val snapshot = LoggingEventVO.build(event)
            if (!queue.offer(snapshot)) {
                // Drop if queue is full to avoid blocking application threads.
            }
        } catch (ex: Exception) {
            addError("Failed to enqueue log event", ex)
        }
    }

    private fun workerLoop() {
        val batch = ArrayList<ILoggingEvent>(BATCH_SIZE)

        while (running.get() || queue.isNotEmpty()) {
            try {
                val first = queue.poll(FLUSH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                if (first != null) {
                    batch.add(first)
                    queue.drainTo(batch, BATCH_SIZE - 1)
                }

                if (batch.isNotEmpty()) {
                    val payload = buildPayload(batch)
                    sendWithRetry(payload)
                    batch.clear()
                }
            } catch (ex: InterruptedException) {
                // Exit if stopped; otherwise continue.
                if (!running.get()) {
                    break
                }
            } catch (ex: Exception) {
                addError("AlertumAppender worker failure", ex)
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
        val mdc = event.mdcPropertyMap
        val traceId = mdc["traceId"]?.takeIf { it.isNotBlank() }
        val spanId = mdc["spanId"]?.takeIf { it.isNotBlank() }
        val parentSpanId = mdc["parentSpanId"]?.takeIf { it.isNotBlank() }
        val requestId = mdc["requestId"]?.takeIf { it.isNotBlank() }
        val teamId = mdc["teamId"]?.takeIf { it.isNotBlank() }
        val userId = mdc["userId"]?.takeIf { it.isNotBlank() }
        val endpoint = mdc["endpoint"]?.takeIf { it.isNotBlank() }
        val httpMethod = mdc["httpMethod"]?.takeIf { it.isNotBlank() }
        val statusCode = mdc["statusCode"]?.toLongOrNull()
        val durationMs = mdc["durationMs"]?.toLongOrNull()
        val tags = mdc["tags"]
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotBlank() }
        val exceptionMessage = event.throwableProxy?.message?.takeIf { it.isNotBlank() }

        val sb = StringBuilder(256)
        sb.append('{')
        var first = true

        fun appendField(name: String, value: String) {
            if (!first) sb.append(',')
            first = false
            sb.append('"').append(name).append('"').append(':')
            sb.append('"').append(escape(value)).append('"')
        }

        fun appendNumber(name: String, value: Long) {
            if (!first) sb.append(',')
            first = false
            sb.append('"').append(name).append('"').append(':').append(value)
        }

        fun appendOptional(name: String, value: String?) {
            if (value == null) return
            appendField(name, value)
        }

        fun appendOptionalNumber(name: String, value: Long?) {
            if (value == null) return
            appendNumber(name, value)
        }

        fun appendArray(name: String, values: List<String>) {
            if (values.isEmpty()) return
            if (!first) sb.append(',')
            first = false
            sb.append('"').append(name).append('"').append(':').append('[')
            values.forEachIndexed { index, item ->
                if (index > 0) sb.append(',')
                sb.append('"').append(escape(item)).append('"')
            }
            sb.append(']')
        }

        appendField("message", event.formattedMessage)
        appendField("level", event.level.levelStr)
        appendField("logger", event.loggerName)
        appendNumber("timestamp", event.timeStamp)
        appendField("service", service)
        appendField("environment", environment)
        appendField("thread", event.threadName)
        appendOptional("traceId", traceId)
        appendOptional("spanId", spanId)
        appendOptional("requestId", requestId)
        appendOptional("teamId", teamId)
        appendOptional("userId", userId)
        appendOptional("endpoint", endpoint)
        appendOptional("httpMethod", httpMethod)
        appendOptionalNumber("statusCode", statusCode)
        appendOptionalNumber("durationMs", durationMs)
        appendOptional("parentSpanId", parentSpanId)
        if (!tags.isNullOrEmpty()) {
            appendArray("tags", tags)
        }

        if (exceptionMessage != null) {
            if (!first) sb.append(',')
            sb.append("\"metadata\":{\"exception\":\"")
                .append(escape(exceptionMessage))
                .append("\"}")
        }

        sb.append('}')
        return sb.toString()
    }

    private fun sendWithRetry(payload: String) {
        if (ingestionKey.isBlank()) return

        val uri = ingestUri()
        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofSeconds(3))
            .header("Content-Type", "application/json")
            .header("X-API-Key", ingestionKey)
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
            if (response.statusCode() !in 200..299) {
                retryOnce(request, response.statusCode())
            }
        } catch (ex: Exception) {
            retryOnce(request, null, ex)
        }
    }

    private fun retryOnce(request: HttpRequest, statusCode: Int? = null, error: Exception? = null) {
        try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
            if (response.statusCode() !in 200..299) {
                addError("AlertumAppender failed to send logs (status=${response.statusCode()})")
            }
        } catch (ex: Exception) {
            val message = statusCode?.let { "status=$it" } ?: "request failed"
            addError("AlertumAppender retry failed ($message)", error ?: ex)
        }
    }

    private fun ingestUri(): URI {
        val base = endpoint.trim().removeSuffix("/")
        return URI.create("$base/api/logs/ingest")
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
                    if (ch < ' ') {
                        sb.append(String.format("\\u%04x", ch.code))
                    } else {
                        sb.append(ch)
                    }
                }
            }
        }
        return sb.toString()
    }

    companion object {
        private const val DEFAULT_ENDPOINT = "http://localhost:8080"
        private const val DEFAULT_SERVICE = "unknown-service"
        private const val DEFAULT_ENVIRONMENT = "local"

        private const val BATCH_SIZE = 50
        private const val FLUSH_INTERVAL_MILLIS = 1000L
        private const val QUEUE_CAPACITY = 10_000
        private const val STOP_JOIN_MILLIS = 3000L
    }
}
