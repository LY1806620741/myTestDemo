package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication2 {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication2.class, new String[]{"--server.port=8081"});
	}

}
