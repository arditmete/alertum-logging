package io.alertum.logging.spring

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import jakarta.servlet.http.HttpServletResponseWrapper
import org.slf4j.MDC
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID
import java.util.concurrent.TimeUnit

class AlertumHttpMdcFilter(
    private val properties: AlertumLoggingProperties
) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val startNanos = System.nanoTime()
        val previousContext = MDC.getCopyOfContextMap()
        val correlationId = request.getHeader(CORRELATION_HEADER)?.trim().orEmpty()
        val traceId = correlationId.takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()
        val responseWrapper = StatusCaptureResponseWrapper(response)

        try {
            setMdcValue("traceId", traceId, overrideExisting = true)
            setMdcValue("httpMethod", request.method ?: "", overrideExisting = true)
            setMdcValue("endpoint", request.requestURI ?: "", overrideExisting = true)
            setMdcValue("statusCode", "0", overrideExisting = true)
            setMdcValue("durationMs", "0", overrideExisting = true)
            setMdcValue("service", properties.serviceOrDefault(), overrideExisting = false)
            setMdcValue("environment", properties.environmentOrDefault(), overrideExisting = false)
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
            } catch (_: Exception) {
                // best effort only
            } finally {
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

    private class StatusCaptureResponseWrapper(response: HttpServletResponse) : HttpServletResponseWrapper(response) {
        private var statusCode: Int = response.status

        override fun setStatus(sc: Int) {
            super.setStatus(sc)
            statusCode = sc
        }

        override fun sendError(sc: Int) {
            super.sendError(sc)
            statusCode = sc
        }

        override fun sendError(sc: Int, msg: String?) {
            super.sendError(sc, msg)
            statusCode = sc
        }

        override fun sendRedirect(location: String?) {
            super.sendRedirect(location)
            statusCode = HttpServletResponse.SC_FOUND
        }

        fun currentStatus(): Int = statusCode
    }

    companion object {
        private const val CORRELATION_HEADER = "X-Correlation-Id"
    }
}
