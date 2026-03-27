package io.alertum.logging.spring

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import jakarta.servlet.http.HttpServletResponseWrapper
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID
import java.util.concurrent.TimeUnit

class AlertumHttpMdcFilter(
    private val properties: AlertumLoggingProperties
) : OncePerRequestFilter() {

    private val accessLog = LoggerFactory.getLogger("io.alertum.http.Access")

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val startNanos = System.nanoTime()
        val previousContext = MDC.getCopyOfContextMap()
        val traceId = resolveOrGenerate(
            request.getHeader(HEADER_TRACE_ID),
            request.getHeader(CORRELATION_HEADER)
        )
        val requestId = resolveOrGenerate(request.getHeader(HEADER_REQUEST_ID))
        val responseWrapper = StatusCaptureResponseWrapper(response) { status ->
            if (status > 0) {
                MDC.put("statusCode", status.toString())
            }
        }

        try {
            setMdcValue("traceId", traceId, overrideExisting = true)
            setMdcValue("requestId", requestId, overrideExisting = true)
            setMdcValue("httpMethod", request.method ?: "", overrideExisting = true)
            setMdcValue("endpoint", request.requestURI ?: "", overrideExisting = true)
            setMdcValue("requestStartMs", System.currentTimeMillis().toString(), overrideExisting = true)
            setMdcValue("service", properties.serviceOrDefault(), overrideExisting = false)
            setMdcValue("environment", properties.environmentOrDefault(), overrideExisting = false)
            val initialStatus = responseWrapper.currentStatus().takeIf { it > 0 }
                ?: HttpServletResponse.SC_OK
            MDC.put("statusCode", initialStatus.toString())
        } catch (_: Exception) {
            // Never block request flow because of MDC issues.
        }

        var failure: Throwable? = null
        try {
            filterChain.doFilter(request, responseWrapper)
        } catch (ex: Throwable) {
            failure = ex
            throw ex
        } finally {
            val durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
            val statusCode = resolveStatusCode(responseWrapper, failure)

            try {
                setMdcValue("statusCode", statusCode.toString(), overrideExisting = true)
                setMdcValue("durationMs", durationMs.toString(), overrideExisting = true)
                setMdcValue("error", (statusCode >= 500).toString(), overrideExisting = true)
            } catch (_: Exception) {
                // best effort only
            } finally {
                if (shouldLogRequest(request)) {
                    val path = request.requestURI ?: "-"
                    val query = request.queryString?.let { "?$it" } ?: ""
                    accessLog.info(
                        "HTTP {} {} status={} durationMs={}",
                        request.method,
                        "$path$query",
                        statusCode,
                        durationMs
                    )
                }
                response.setHeader(HEADER_TRACE_ID, traceId)
                response.setHeader(HEADER_REQUEST_ID, requestId)
                restoreMdc(previousContext)
            }
        }
    }

    private fun setMdcValue(key: String, value: String, overrideExisting: Boolean) {
        val existing = MDC.get(key)
        if (overrideExisting || existing.isNullOrBlank()) {
            MDC.put(key, value)
        }
    }

    private fun resolveStatusCode(response: StatusCaptureResponseWrapper, failure: Throwable?): Int {
        val current = response.currentStatus()
        return if (failure != null && current < 400) HttpServletResponse.SC_INTERNAL_SERVER_ERROR else current
    }

    private fun restoreMdc(previousContext: Map<String, String>?) {
        if (previousContext.isNullOrEmpty()) {
            MDC.clear()
        } else {
            MDC.setContextMap(previousContext)
        }
    }

    private fun resolveOrGenerate(vararg candidates: String?): String {
        for (candidate in candidates) {
            val trimmed = candidate?.trim()
            if (!trimmed.isNullOrBlank()) {
                return trimmed
            }
        }
        return UUID.randomUUID().toString()
    }

    private fun shouldLogRequest(request: HttpServletRequest): Boolean {
        if (request.method == "OPTIONS") return false
        return request.requestURI != null
    }

    private class StatusCaptureResponseWrapper(
        response: HttpServletResponse,
        private val statusObserver: (Int) -> Unit
    ) : HttpServletResponseWrapper(response) {
        private var statusCode: Int = response.status

        override fun setStatus(sc: Int) {
            super.setStatus(sc)
            statusCode = sc
            statusObserver(sc)
        }

        override fun sendError(sc: Int) {
            super.sendError(sc)
            statusCode = sc
            statusObserver(sc)
        }

        override fun sendError(sc: Int, msg: String?) {
            super.sendError(sc, msg)
            statusCode = sc
            statusObserver(sc)
        }

        override fun sendRedirect(location: String?) {
            super.sendRedirect(location)
            statusCode = HttpServletResponse.SC_FOUND
            statusObserver(statusCode)
        }

        fun currentStatus(): Int = statusCode
    }

    companion object {
        private const val HEADER_TRACE_ID = "X-Trace-Id"
        private const val HEADER_REQUEST_ID = "X-Request-Id"
        private const val CORRELATION_HEADER = "X-Correlation-Id"
    }
}
