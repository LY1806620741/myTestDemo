package com.example.demo;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

@RestController
@Log4j2
public class WebController implements CommandLineRunner {

    @Autowired
    CoordinatorRegistryCenter regCenter;

    @Autowired
    MySimpleJob mySimpleJob;

    HashMap<String,SpringJobScheduler> passiveMap;

    HashMap<String,SpringJobScheduler> activeMap;

//    @PostConstruct
    public void init() {
        List<String> childrenKeys = regCenter.getChildrenKeys("/");
        childrenKeys.forEach(jobName -> {
            String s = regCenter.get("/" + jobName + "/config");
            if (null == s) {
                String s1 = regCenter.get("/" + jobName);
                if (null!= s1){
                    regCenter.remove("/" + jobName);
                }
                return;
            }
            JSONObject jsonObject = JSON.parseObject(s);
            log.info("加载任务 {}",s);
            JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jsonObject.getString("jobName"), jsonObject.getString("cron"), jsonObject.getInteger("shardingTotalCount")).shardingItemParameters(jsonObject.getString("shardingItemParameters")).build();
            SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, jsonObject.getString("jobClass"));
            try {
                new SpringJobScheduler(mySimpleJob, regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build()).init();
            } catch (Exception ex) {
                if (ex.getMessage().endsWith("will never fire.")) {
                    log.info("{} 过时了，尝试触发并删除",jobName);
                    try {
                        JobRegistry.getInstance().getJobScheduleController(jobName).triggerJob(); //触发时不存在
                    } catch (Exception exc) {
                        log.error(exc);
                    }
                    JobRegistry.getInstance().shutdown(jobName);
                    regCenter.remove("/" + jobName);
                }else{
                    JobRegistry.getInstance().shutdown(jobName);
                    regCenter.remove("/" + jobName);
                    log.error("未知异常",ex);
                }
            }
        });
        monitorJobRegister();
    }

    @GetMapping("/init")
    public void registerScheduleList() {
        IntStream.range(0, 5).forEach(i -> {
            start(i);
        });
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
                    String jobName = jsonObject.getString("jobName");
                    try {

                        if (null == JobRegistry.getInstance().getJobInstance(jobName)) {
//                        if (null == JobRegistry.getInstance().getJobScheduleController(jobName)) {//因为elasticJob先注册zk再启动job，所以这个不能检测本机是否启动了任务无效
                            log.info("{} 启动，且未检测到本机包含该任务，尝试从本机也启动",jobName);
                            JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(jobName, jsonObject.getString("cron"), jsonObject.getInteger("shardingTotalCount")).shardingItemParameters(jsonObject.getString("shardingItemParameters")).build();
                            SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, jsonObject.getString("jobClass"));
                            new SpringJobScheduler(mySimpleJob, regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build()).init();
                        }
                    } catch (Exception e) {
                        log.error("监听到任务服务被创建",e);
                    }
                    break;
                case CHILD_REMOVED:
                    jobName = data.getPath().substring(1);
                    if (null != JobRegistry.getInstance().getJobScheduleController(jobName)) {
                        log.info("{} 被其他移除，尝试从本机注销",jobName);
                        JobRegistry.getInstance().shutdown(jobName);
                        regCenter.remove("/" + jobName);
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

    Random random = new Random(10);

    @GetMapping("/start/{id}")
    public Mono<String> start(@PathVariable("id") Integer id) {
        String jobName = "javaSimpleJob" + id;
        log.info("主动创建任务{}",jobName);
//        CoordinatorRegistryCenter regCenter = JobRegistry.getInstance().getRegCenter("/");
        JobCoreConfiguration coreConfig = JobCoreConfiguration
//                .newBuilder(jobName, getCron(Date.from(Instant.now().plus(random.nextInt(180) - 40, ChronoUnit.SECONDS))), 1)
                .newBuilder(jobName, getCron(Date.from(Instant.now().plus(random.nextInt(10) - 2, ChronoUnit.SECONDS))), 1)
                .shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou")
                .description("简介")
                .jobParameter(random.nextInt(3)>2?"二阶段":"一阶段")
                .build();
        SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(coreConfig, MySimpleJob.class.getCanonicalName());
        try {
            new SpringJobScheduler(mySimpleJob, regCenter, LiteJobConfiguration.newBuilder(simpleJobConfig).build(), new ElasticJobListener() {
                @Override
                public void beforeJobExecuted(ShardingContexts shardingContexts) {

                }

                @Override
                public void afterJobExecuted(ShardingContexts shardingContexts) {
                    log.info("{} 任务结束后删除自身",shardingContexts.getJobName());
                    regCenter.remove("/" + jobName);
                }
            }).init();
        } catch (Exception ex) {
            if (ex.getMessage().endsWith("will never fire.")) {
                JobRegistry.getInstance().shutdown(jobName);
                regCenter.remove("/" + jobName);

            }else {
                throw new RuntimeException(ex);
            }
        }
        return Mono.just("");
    }

    @GetMapping("/delete/{id}")
    public Mono<String> delete(@PathVariable("id") Integer id) {
//        CoordinatorRegistryCenter regCenter = JobRegistry.getInstance().getRegCenter("/");
        String jobName = "javaSimpleJob" + id;
        log.info("主动删除任务{}",jobName);
//        Optional.ofNullable(JobRegistry.getInstance().getJobScheduleController(s)).ifPresent(JobScheduleController::shutdown);
        JobRegistry.getInstance().shutdown(jobName);
        regCenter.remove("/" + jobName);
        return Mono.just("");
    }

    @GetMapping("/")
    public Mono<String> index() {
        return Mono.just("hello");
    }

    public void print(String str) {
        log.info("{} execute ", str);
    }

    public static String getCron(Date date) {
        String dateFormat = "ss mm HH dd MM ? yyyy";
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        String formatTimeStr = null;
        if (date != null) {
            formatTimeStr = sdf.format(date);
        }
        return formatTimeStr;
    }

    @Override
    public void run(String... args) throws Exception {
        init();
    }
}
