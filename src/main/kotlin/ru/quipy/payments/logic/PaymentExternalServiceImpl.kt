package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentMetrics: PaymentMetrics,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private const val TEMPORARY_ERROR = "Temporary error"

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val minimumRequestBudgetMillis = requestAverageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val retryDelaysMillis = longArrayOf(25L, 75L)

    private var rateLimiter: SlidingWindowRateLimiter
    private var inflightLimiter: OngoingWindow

    init {
        val parsedRateLimit = rateLimitPerSec.toLong()
        val duration = Duration.ofSeconds(1)

        rateLimiter = SlidingWindowRateLimiter(parsedRateLimit, duration)
        inflightLimiter = OngoingWindow(parallelRequests)
    }


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        fun tooLate(submitted: Boolean) =
            rejectIfTooLate(paymentId, paymentStartedAt, deadline, transactionId, submitted)

        try {
            for (attempt in 0..retryDelaysMillis.size) {
                val firstAttempt = attempt == 0
                if (tooLate(submitted = !firstAttempt)) return

                val body = withRequestSlot {
                    if (tooLate(submitted = !firstAttempt)) return
                    if (firstAttempt) {
                        recordSubmission(paymentId, transactionId, paymentStartedAt)
                        if (tooLate(submitted = true)) return
                    }
                    requestPayment(paymentId, transactionId, amount, deadline)
                } ?: return

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, attempt=${attempt + 1}")
                recordProviderError(body)
                if (body.result || body.message != TEMPORARY_ERROR || attempt == retryDelaysMillis.size) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return
                }
                Thread.sleep(retryDelaysMillis[attempt])
            }
        } catch (e: InterruptedIOException) {
            paymentMetrics.recordClientError(accountName, "timeout")
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
        } catch (e: Exception) {
            paymentMetrics.recordClientError(accountName, "exception")
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
        }
    }

    private inline fun <T> withRequestSlot(action: () -> T): T {
        inflightLimiter.acquire()
        try {
            rateLimiter.tickBlocking()
            return action()
        } finally {
            inflightLimiter.release()
        }
    }

    private fun recordSubmission(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long) {
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
    }

    private fun requestPayment(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): ExternalSysResponse? {
        val timeoutMillis = (deadline - now()).coerceAtLeast(1L)
        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        return client.newBuilder()
            .callTimeout(Duration.ofMillis(timeoutMillis))
            .build()
            .newCall(request).execute().use { response ->
                val responseText = response.body?.string()
                try {
                    mapper.readValue(responseText, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseText", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }
    }

    private fun recordProviderError(body: ExternalSysResponse) {
        if (body.result) return
        val reason = when (body.message) {
            TEMPORARY_ERROR -> "temporary_error"
            null -> "unknown"
            else -> "provider_error"
        }
        paymentMetrics.recordClientError(accountName, reason)
    }

    private fun rejectIfTooLate(
        paymentId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        submissionAlreadyRecorded: Boolean,
    ): Boolean {
        val checkedAt = now()
        if (deadline - checkedAt > minimumRequestBudgetMillis) {
            return false
        }

        if (!submissionAlreadyRecorded) {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = false,
                    transactionId = transactionId,
                    startedAt = checkedAt,
                    spentInQueueDuration = Duration.ofMillis(checkedAt - paymentStartedAt),
                )
            }
        }
        paymentESService.update(paymentId) {
            it.logProcessing(
                success = false,
                processedAt = now(),
                transactionId = transactionId,
                reason = "Not enough time before payment deadline",
            )
        }
        paymentMetrics.recordClientError(accountName, "deadline")
        return true
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
