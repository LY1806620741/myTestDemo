package com.example.demo;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import jakarta.annotation.PostConstruct;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.quartz.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

@RestController
public class WebController {

    @Autowired
    CoordinatorRegistryCenter regCenter;

    @PostConstruct
    public void init() {
        List<String> childrenKeys = regCenter.getChildrenKeys("/");
        childrenKeys.forEach(e -> {
            String s = regCenter.get("/" + e + "/config");
            if (null == s){
                return;
            }
            JSONObject jsonObject = JSON.parseObject(s);
            System.out.println(s);
            JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jsonObject.getString("jobName"), jsonObject.getString("cron"), jsonObject.getInteger("shardingTotalCount")).shardingItemParameters(jsonObject.getString("shardingItemParameters")).build();
            SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, jsonObject.getString("jobClass"));
            new JobScheduler(regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build()).init();
        });
        monitorJobRegister();
    }

    public void monitorJobRegister() {
        CuratorFramework client = (CuratorFramework) regCenter.getRawClient();
        PathChildrenCache childrenCache = new PathChildrenCache(client, "/", true);
        PathChildrenCacheListener childrenCacheListener = (client1, event) -> {
            ChildData data = event.getData();
            switch (event.getType()) {
                case CHILD_ADDED:
                    String config = new String(client1.getData().forPath(data.getPath() + "/config"));
                    JSONObject jsonObject = JSON.parseObject(config);
                    if (null==JobRegistry.getInstance().getJobScheduleController(jsonObject.getString("jobName"))) {
                        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jsonObject.getString("jobName"), jsonObject.getString("cron"), jsonObject.getInteger("shardingTotalCount")).shardingItemParameters(jsonObject.getString("shardingItemParameters")).build();
                        SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, jsonObject.getString("jobClass"));
                        new JobScheduler(regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build()).init();
                    }

                    break;
                case CHILD_REMOVED:
                    String substring = data.getPath().substring(1);
                    if (null!=JobRegistry.getInstance().getJobScheduleController(substring)) {
                        JobRegistry.getInstance().shutdown(substring);
                        regCenter.remove("/" + substring);
                    }

                default:
                    break;
            }
        };
        childrenCache.getListenable().addListener(childrenCacheListener);
        try {
            // https://blog.csdn.net/u010402202/article/details/79581575
            childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/start/{id}")
    public Mono<String> test(@PathVariable("id") Integer id) {
//        CoordinatorRegistryCenter regCenter = JobRegistry.getInstance().getRegCenter("/");
        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder("javaSimpleJob" + id, "0/5 * * * * ?", 3).shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou").build();
        SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, MySimpleJob.class.getCanonicalName());
        new JobScheduler(regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build()).init();
        return Mono.just("");
    }

    @GetMapping("/delete/{id}")
    public Mono<String> delete(@PathVariable("id") Integer id) {
//        CoordinatorRegistryCenter regCenter = JobRegistry.getInstance().getRegCenter("/");
        String s = "javaSimpleJob" + id;
//        Optional.ofNullable(JobRegistry.getInstance().getJobScheduleController(s)).ifPresent(JobScheduleController::shutdown);
        JobRegistry.getInstance().shutdown(s);
        regCenter.remove("/" + s);
        return Mono.just("");
    }

    @GetMapping("/")
    public Mono<String> index() {
        return Mono.just("hello");
    }
}
