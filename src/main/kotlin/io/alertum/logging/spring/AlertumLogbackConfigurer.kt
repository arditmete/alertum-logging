package io.alertum.logging.spring

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import io.alertum.logging.AlertumAppender
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.event.EventListener

class AlertumLogbackConfigurer(
    private val properties: AlertumLoggingProperties
) {

    @EventListener(ApplicationStartedEvent::class)
    fun applyPropertiesToAppenders() {
        val context = LoggerFactory.getILoggerFactory() as? LoggerContext ?: return
        val configured = LinkedHashSet<AlertumAppender>()

        for (logger in context.loggerList) {
            val iterator = logger.iteratorForAppenders()
            while (iterator.hasNext()) {
                val appender = iterator.next()
                if (appender is AlertumAppender && configured.add(appender)) {
                    applyProperties(appender)
                }
            }
        }

        val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
        val rootIterator = root.iteratorForAppenders()
        while (rootIterator.hasNext()) {
            val appender = rootIterator.next()
            if (appender is AlertumAppender && configured.add(appender)) {
                applyProperties(appender)
            }
        }
    }

    private fun applyProperties(appender: AlertumAppender) {
        properties.apiKeyOrNull()?.let {
            appender.setIngestionKey(it)
        }

        properties.endpoint?.trim()?.takeIf { it.isNotBlank() }?.let {
            appender.setEndpoint(it)
        }
        properties.service?.trim()?.takeIf { it.isNotBlank() }?.let {
            appender.setService(it)
        }
        properties.environment?.trim()?.takeIf { it.isNotBlank() }?.let {
            appender.setEnvironment(it)
        }

        if (!appender.isStarted) {
            appender.start()
        }
    }
}
