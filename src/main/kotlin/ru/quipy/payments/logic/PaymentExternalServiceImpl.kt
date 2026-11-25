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
import java.net.SocketTimeoutException
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

        var finalBody: ExternalSysResponse? = null
        var deadlineRejected = false
        var submitted = false
        try {
            for (attempt in 0 until 3) {
                if (rejectIfTooLate(paymentId, paymentStartedAt, deadline, transactionId, submitted)) {
                    deadlineRejected = true
                    break
                }

                inflightLimiter.acquire()
                try {
                    rateLimiter.tickBlocking()

                    if (rejectIfTooLate(paymentId, paymentStartedAt, deadline, transactionId, submitted)) {
                        deadlineRejected = true
                        break
                    }

                    if (attempt == 0) {
                        paymentESService.update(paymentId) {
                            it.logSubmission(
                                success = true,
                                transactionId,
                                now(),
                                Duration.ofMillis(now() - paymentStartedAt),
                            )
                        }
                        submitted = true
                        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

                        if (rejectIfTooLate(paymentId, paymentStartedAt, deadline, transactionId, submitted)) {
                            deadlineRejected = true
                            break
                        }
                    }

                    val timeoutMillis = (deadline - now()).coerceAtLeast(1L)
                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    client.newBuilder()
                        .callTimeout(Duration.ofMillis(timeoutMillis))
                        .build()
                        .newCall(request).execute().use { response ->
                            val responseText = response.body?.string()
                            finalBody = try {
                                mapper.readValue(responseText, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseText", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                        }
                } finally {
                    inflightLimiter.release()
                }

                val body = finalBody ?: break
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, attempt=${attempt + 1}")

                if (!body.result) {
                    paymentMetrics.recordClientError(
                        accountName,
                        when (body.message) {
                            "Temporary error" -> "temporary_error"
                            null -> "unknown"
                            else -> "provider_error"
                        }
                    )
                }

                if (body.result || body.message != "Temporary error" || attempt == 2) break
                Thread.sleep(retryDelaysMillis[attempt])
            }

            if (!deadlineRejected) finalBody?.let { body ->
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException, is InterruptedIOException -> {
                    paymentMetrics.recordClientError(accountName, "timeout")
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    paymentMetrics.recordClientError(accountName, "exception")
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
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
