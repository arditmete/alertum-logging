package io.alertum.logging.spring

import ch.qos.logback.classic.LoggerContext
import io.alertum.logging.AlertumAppender
import org.springframework.boot.autoconfigure.condition.*
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.Ordered
import org.springframework.web.filter.OncePerRequestFilter
@Configuration
@EnableConfigurationProperties(AlertumLoggingProperties::class)
@ConditionalOnClass(AlertumAppender::class)
@ConditionalOnProperty(
        prefix = "alertum.logging",
        name = ["enabled"],
        havingValue = "true",
        matchIfMissing = true
)
open class AlertumLoggingAutoConfiguration {

    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(OncePerRequestFilter::class)
    @ConditionalOnMissingBean
    open fun alertumHttpMdcFilter(
            properties: AlertumLoggingProperties
    ): AlertumHttpMdcFilter {
        return AlertumHttpMdcFilter(properties)
    }

    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(FilterRegistrationBean::class)
    @ConditionalOnMissingBean
    open fun alertumHttpMdcFilterRegistration(
            filter: AlertumHttpMdcFilter
    ): FilterRegistrationBean<AlertumHttpMdcFilter> {
        return FilterRegistrationBean(filter).apply {
            order = Ordered.HIGHEST_PRECEDENCE + FILTER_ORDER_OFFSET
        }
    }

    @Bean
    @ConditionalOnClass(LoggerContext::class)
    @ConditionalOnMissingBean
    open fun alertumLogbackConfigurer(
            properties: AlertumLoggingProperties
    ): AlertumLogbackConfigurer {
        return AlertumLogbackConfigurer(properties)
    }

    companion object {
        private const val FILTER_ORDER_OFFSET = 10
    }
}