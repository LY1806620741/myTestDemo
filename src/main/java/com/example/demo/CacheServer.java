package com.example.demo;

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import org.springframework.cache.annotation.Cacheable;

public class CacheServer {

    @Cacheable("")
    public String test(String key){
//        JobRegistry.getInstance()
        return "";
    }
}
