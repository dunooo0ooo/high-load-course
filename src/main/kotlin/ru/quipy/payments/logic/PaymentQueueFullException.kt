package ru.quipy.payments.logic

class PaymentQueueFullException(
    val retryAfter: Long
) : RuntimeException("Payment queue is full")
