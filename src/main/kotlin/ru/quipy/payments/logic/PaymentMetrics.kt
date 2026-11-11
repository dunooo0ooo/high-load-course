package ru.quipy.payments.logic

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(meterRegistry: MeterRegistry) {
    private val registry = meterRegistry
    private val processingDuration = DistributionSummary
        .builder("payment.processing.duration")
        .description("Time from accepted payment submission to processing completion")
        .baseUnit("seconds")
        .publishPercentiles(0.5, 0.75, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    fun observeRequestDuration(milliseconds: Long) {
        processingDuration.record(milliseconds.coerceAtLeast(0).toDouble() / 1000.0)
    }

    fun recordClientError(account: String, reason: String) {
        Counter.builder("payment.client.errors")
            .description("Payment errors returned by the external payment client")
            .tag("account", account)
            .tag("reason", reason)
            .register(registry)
            .increment()
    }
}
