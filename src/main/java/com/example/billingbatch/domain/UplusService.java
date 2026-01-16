package com.example.billingbatch.domain;

public record UplusService(
  Long id,
  String name,
  String code,
  Long type_id,
  Long price,
  String description
) {}
