package io.alertum.logging.spring

import io.alertum.logging.AlertumDefaults
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "alertum.logging")
class AlertumLoggingProperties {

    var enabled: Boolean = true
    var apiKey: String? = null
    var endpoint: String? = null
    var service: String? = null
    var environment: String? = null

    fun apiKeyOrNull(): String? =
            apiKey?.trim()?.takeIf { it.isNotBlank() }

    fun endpointOrDefault(): String =
            endpoint?.trim().takeUnless { it.isNullOrBlank() }
                    ?: AlertumDefaults.DEFAULT_ENDPOINT

    fun serviceOrDefault(): String =
            service?.trim().takeUnless { it.isNullOrBlank() }
                    ?: AlertumDefaults.DEFAULT_SERVICE

    fun environmentOrDefault(): String =
            environment?.trim().takeUnless { it.isNullOrBlank() }
                    ?: AlertumDefaults.DEFAULT_ENVIRONMENT
}