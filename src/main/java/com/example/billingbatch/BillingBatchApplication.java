package com.example.billingbatch;

import jakarta.annotation.PostConstruct;
import java.util.TimeZone;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BillingBatchApplication {

  @PostConstruct
  public void started() {
    // 기본 시간대를 한국 시간으로 설정
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
  }
  public static void main(String[] args) {
    SpringApplication.run(BillingBatchApplication.class, args);

  }

}
