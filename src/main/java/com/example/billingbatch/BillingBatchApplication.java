package com.example.billingbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
//@EnableScheduling // TODO 스케줄러&테이블명 수정 이후 주석 지우기
public class BillingBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(BillingBatchApplication.class, args);

	}

}
