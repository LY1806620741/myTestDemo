package com.example.demo;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.base.RegistryCenter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Random;

import static com.example.demo.WebController.getCron;

@Component
@Log4j2
public class MySimpleJob implements SimpleJob, ElasticJobListener {

    @Autowired
    WebController webController;

    @Autowired
    CoordinatorRegistryCenter regCenter;

    Random random = new Random(10);

    @Override
    public void execute(ShardingContext shardingContext) {
        log.info("[{}] execute ", shardingContext.getJobName());

        try {
            webController.print(shardingContext.getJobName());
        } catch (Exception e) {
            log.error("未知的错误", e);
        }
    }

    @Override
    public void beforeJobExecuted(ShardingContexts shardingContexts) {

    }

    @Override
    public void afterJobExecuted(ShardingContexts shardingContexts) {
        String taskId = shardingContexts.getTaskId();
//        afterJobExecuted会在所有集群调用  且调度时间可能不一致 javaSimpleJob4@-@0@-@READY@-@10.100.128.56@-@5996
        if (taskId.contains(JobRegistry.getInstance().getJobInstance(shardingContexts.getJobName()).getJobInstanceId())) {

            log.info("{} afterJobExecuted ", shardingContexts.getJobName());

//            if (shardingContexts.getJobParameter().startsWith("二")) {
        if (shardingContexts.getJobParameter().startsWith("二")&&random.nextInt(10)>5){
                String cron = getCron(Date.from(Instant.now().plus(random.nextInt(10) + 2, ChronoUnit.SECONDS)));
                log.info("{} 进入二阶段 下次调度 {} ", shardingContexts.getJobName(), cron);
                String id = shardingContexts.getJobName().replace("javaSimpleJob", "");
//            webController.start(Integer.valueOf(id)); com.dangdang.ddframe.job.executor.JobFacade.failoverIfNecessary
                JobRegistry.getInstance().getJobScheduleController(shardingContexts.getJobName()).rescheduleJob(cron);
            } else {
                log.info("{} 任务结束后删除自身", shardingContexts.getJobName());
                JobRegistry.getInstance().shutdown(shardingContexts.getJobName());
                regCenter.remove("/" + shardingContexts.getJobName()); //如果在任务执行里删除，后面elasticJob再清除自身节点时因为路径不存在会异常
            }
        }else{
            log.info("{} afterJobExecuted 非当前集群,忽略 {}",shardingContexts.getJobName(),shardingContexts.getTaskId());
        }

    }
}
