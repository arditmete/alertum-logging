package io.alertum.logging

import io.alertum.logs.v1.LogBatchRequest
import io.alertum.logs.v1.LogBatchResponse
import io.alertum.logs.v1.LogEntry
import io.alertum.logs.v1.LogsIngestionServiceGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class AlertumGrpcTransport(
    host: String,
    port: Int,
    apiKey: String?,
    private val sdkVersion: String,
    private val source: String,
    private val onSuccess: (Int) -> Unit,
    private val onFailure: (Int, Status.Code?) -> Unit,
    private val onRetry: () -> Unit
) {

    private val closed = AtomicBoolean(false)
    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "alertum-grpc-retry").apply { isDaemon = true }
    }

    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build()

    private val stub: LogsIngestionServiceGrpc.LogsIngestionServiceStub = run {
        var base = LogsIngestionServiceGrpc.newStub(channel)
        val trimmed = apiKey?.trim().orEmpty()
        if (trimmed.isNotEmpty()) {
            val metadata = Metadata()
            metadata.put(API_KEY_HEADER, trimmed)
            base = base.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
        }
        base
    }

    fun send(batch: List<LogEntry>) {
        if (batch.isEmpty() || closed.get()) return
        sendAttempt(batch, 1, 0L)
    }

    fun shutdown(timeoutMs: Long) {
        closed.set(true)
        scheduler.shutdownNow()
        channel.shutdown()
        channel.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)
    }

    private fun sendAttempt(batch: List<LogEntry>, attempt: Int, delayMs: Long) {
        if (closed.get()) return
        if (delayMs > 0L) {
            try {
                scheduler.schedule({ doSend(batch, attempt) }, delayMs, TimeUnit.MILLISECONDS)
            } catch (_: Exception) {
                doSend(batch, attempt)
            }
            return
        }
        doSend(batch, attempt)
    }

    private fun doSend(batch: List<LogEntry>, attempt: Int) {
        if (closed.get()) return
        val request = LogBatchRequest.newBuilder()
            .setSdkVersion(sdkVersion)
            .setSource(source)
            .addAllLogs(batch)
            .build()
        stub.withDeadlineAfter(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .sendLogs(request, object : StreamObserver<LogBatchResponse> {
                override fun onNext(value: LogBatchResponse) {
                    // no-op
                }

                override fun onError(t: Throwable) {
                    val status = Status.fromThrowable(t)
                    val code = status.code
                    if (shouldRetry(code) && attempt < MAX_ATTEMPTS && !closed.get()) {
                        onRetry.invoke()
                        val delay = withJitter(backoffDelay(attempt))
                        sendAttempt(batch, attempt + 1, delay)
                        return
                    }
                    onFailure.invoke(batch.size, code)
                }

                override fun onCompleted() {
                    onSuccess.invoke(batch.size)
                }
            })
    }

    private fun shouldRetry(code: Status.Code): Boolean {
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED
    }

    private fun backoffDelay(attempt: Int): Long {
        val base = INITIAL_BACKOFF_MS * (1L shl (attempt - 1))
        return minOf(base, MAX_BACKOFF_MS)
    }

    private fun withJitter(delayMs: Long): Long {
        if (delayMs <= 0L) return 0L
        val extra = ThreadLocalRandom.current().nextLong(0L, delayMs + 1)
        return minOf(delayMs + extra, MAX_BACKOFF_MS)
    }

    companion object {
        private const val MAX_ATTEMPTS = 3
        private const val INITIAL_BACKOFF_MS = 200L
        private const val MAX_BACKOFF_MS = 5_000L
        private const val REQUEST_TIMEOUT_MS = 2_000L

        private val API_KEY_HEADER: Metadata.Key<String> =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)
    }
}
