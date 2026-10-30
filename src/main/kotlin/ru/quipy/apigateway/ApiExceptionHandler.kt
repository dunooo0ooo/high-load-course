package ru.quipy.apigateway

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.quipy.payments.logic.PaymentQueueFullException

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(PaymentQueueFullException::class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    fun handlePaymentQueueFull() = Unit
}
