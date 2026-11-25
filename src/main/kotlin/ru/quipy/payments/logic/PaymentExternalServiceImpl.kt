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
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentMetrics: PaymentMetrics,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private const val RESPONSE_GRACE_MILLIS = 200L
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

    private val attemptTimeoutMillis = (minimumRequestBudgetMillis * 3 / 2).coerceAtLeast(1L)
    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false) // Handle HTTP 408 in the payment retry loop.
        .eventListenerFactory(paymentMetrics.attemptLatency(accountName))
        .build()
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

        fun tooLate(submitted: Boolean, checkedAt: Long = now()) =
            rejectIfTooLate(paymentId, paymentStartedAt, deadline, transactionId, submitted, checkedAt)

        try {
            for (attempt in 0..retryDelaysMillis.size) {
                val firstAttempt = attempt == 0
                if (tooLate(submitted = !firstAttempt)) return

                val result = withRequestSlot {
                    if (firstAttempt) {
                        if (tooLate(submitted = false)) return
                        recordSubmission(paymentId, transactionId, paymentStartedAt)
                    }
                    val checkedAt = now()
                    if (tooLate(submitted = true, checkedAt = checkedAt)) return
                    requestPayment(paymentId, transactionId, amount, deadline - checkedAt, attempt)
                } ?: return

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${result.success}, message: ${result.message}, attempt=${attempt + 1}")
                if (!result.retryable || attempt == retryDelaysMillis.size) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(result.success, now(), transactionId, reason = result.message)
                    }
                    return
                }
                Thread.sleep(retryDelaysMillis[attempt])
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

    private data class AttemptResult(val success: Boolean, val message: String?, val retryable: Boolean)

    private fun requestPayment(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        remainingMillis: Long,
        attempt: Int,
    ): AttemptResult? {
        val providerMillis = minOf(attemptTimeoutMillis, remainingMillis - RESPONSE_GRACE_MILLIS)
        val timeout = Duration.ofMillis(providerMillis)
        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
            .post(emptyBody)
            .build()
        val call = client.newCall(request)
        call.timeout().timeout(providerMillis + RESPONSE_GRACE_MILLIS, TimeUnit.MILLISECONDS)
        if (attempt > 0) paymentMetrics.recordRetry(accountName)

        try {
            return call.execute().use { response ->
                if (response.code == 408) return timeoutResult("Provider timeout.")
                val responseText = response.body?.string()
                val body = try {
                    mapper.readValue(responseText, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseText", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                } ?: return null
                recordProviderError(body)
                AttemptResult(body.result, body.message, !body.result && body.message == TEMPORARY_ERROR)
            }
        } catch (e: InterruptedIOException) {
            return timeoutResult("Request timeout.")
        }
    }

    private fun timeoutResult(reason: String): AttemptResult {
        paymentMetrics.recordTimeout(accountName)
        return AttemptResult(false, reason, retryable = true)
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
        checkedAt: Long,
    ): Boolean {
        if (deadline - checkedAt > maxOf(minimumRequestBudgetMillis, RESPONSE_GRACE_MILLIS)) {
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
