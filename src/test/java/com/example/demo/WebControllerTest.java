package com.example.demo;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

class WebControllerTest {

    @Test
    void test(){
        System.out.println(DateTimeFormatter.ofPattern("ss mm */2 * * ? *")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now()));
    }

}