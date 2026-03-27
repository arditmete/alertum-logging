package io.alertum.logging.spring

import io.alertum.logging.AlertumDefaults
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "alertum.logging")
class AlertumLoggingProperties {

    var enabled: Boolean = true
    var teamId: String? = null
    var service: String? = null
    var environment: String? = null

    fun teamIdOrNull(): String? =
            teamId?.trim()?.takeIf { it.isNotBlank() }

    fun serviceOrDefault(): String =
            service?.trim().takeUnless { it.isNullOrBlank() }
                    ?: AlertumDefaults.DEFAULT_SERVICE

    fun environmentOrDefault(): String =
            environment?.trim().takeUnless { it.isNullOrBlank() }
                    ?: AlertumDefaults.DEFAULT_ENVIRONMENT
}
