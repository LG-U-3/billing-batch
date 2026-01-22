package com.example.billingbatch.jobs.reservation.processor.dto;

public record ReservationInfo(
    Long reservationId,
    Long templateId,
    Long channelTypeId,
    Long purposeTypeId
) {

}