package com.example.billingbatch.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageSendResult {

  private Long id;
  private Long reservedSendId;
  private Long userId;
  private Long templateId;
  private Long statusId;
  private Long channelId;
}
