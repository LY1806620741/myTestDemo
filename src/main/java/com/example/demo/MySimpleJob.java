package com.example.demo;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
@Log4j2
public class MySimpleJob implements SimpleJob {

    @Autowired
    WebController webController;

    @Autowired
    CoordinatorRegistryCenter regCenter;

    Random random = new Random(10);


    @Override
    public void execute(ShardingContext shardingContext) {
        try {
            webController.print(shardingContext.getJobName());
        } catch (Exception e) {
            log.error("未知的错误",e);
        }finally {
            if (shardingContext.getJobParameter().startsWith("二")){
//                String cron = getCron(Date.from(Instant.now().plus(random.nextInt(180) - 40, ChronoUnit.SECONDS)));
                log.info("进入二阶段 下次调度 ");
                String id = shardingContext.getJobName().replace("javaSimpleJob", "");
                webController.start(Integer.valueOf(id));
            }else {
                log.info("{} 任务结束后删除自身",shardingContext.getJobName());
                JobRegistry.getInstance().shutdown(shardingContext.getJobName());
//                regCenter.remove("/" + shardingContext.getJobName()); //如果在任务里删除，后面elasticJob再清除自身节点时因为路径不存在会异常
            }
        }

    }
}
