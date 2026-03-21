package io.alertum.logging.spring

import io.alertum.logging.AlertumDefaults
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "alertum")
class AlertumLoggingProperties {
    var service: String? = null
    var environment: String? = null
    var endpoint: String? = null

    fun serviceOrDefault(): String {
        val value = service?.trim()
        return if (value.isNullOrBlank()) AlertumDefaults.DEFAULT_SERVICE else value
    }

    fun environmentOrDefault(): String {
        val value = environment?.trim()
        return value ?: AlertumDefaults.DEFAULT_ENVIRONMENT
    }

    fun endpointOrDefault(): String {
        val value = endpoint?.trim()
        return if (value.isNullOrBlank()) AlertumDefaults.DEFAULT_ENDPOINT else value
    }

    fun hasService(): Boolean = !service.isNullOrBlank()

    fun hasEnvironment(): Boolean = !environment.isNullOrBlank()

    fun hasEndpoint(): Boolean = !endpoint.isNullOrBlank()
}
