package com.example.billingbatch.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class UplusService {
  Long id;
  String name;
  String code;
  Long type_id;
  Long price;
  String description;
}
