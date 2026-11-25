package ru.quipy.apigateway

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import ru.quipy.payments.logic.PaymentQueueFullException


@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(PaymentQueueFullException::class)
    fun handlePaymentQueueFull(exception: PaymentQueueFullException): ResponseEntity<Void> =
        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(
                HttpHeaders.RETRY_AFTER,
                exception.retryAfter.toString()
            )
            .build()
}
