package com.example.demo;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.event.JobEventConfiguration;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.lite.internal.schedule.LiteJobFacade;
import com.dangdang.ddframe.job.lite.spring.job.util.AopTargetUtils;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Optional;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.function.SupplierUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler
 */
public class SpringJobSchedulerEnhance extends JobScheduler {

    private static Method createScheduler = Mono.just(ReflectionUtils.findMethod(SpringJobSchedulerEnhance.class, "createScheduler")).doOnNext(e -> ReflectionUtils.makeAccessible(e)).block();
    private static Method createJobDetail = SupplierUtils.resolve(() -> {
        Method method = ReflectionUtils.findMethod(SpringJobSchedulerEnhance.class, "createJobDetail", String.class);
        ReflectionUtils.makeAccessible(method);
        return method;
    });

    private static Field jobDetailField = Mono.just(ReflectionUtils.findField(JobScheduleController.class, "jobDetail")).doOnNext(self ->
            ReflectionUtils.makeAccessible(self)
    ).block();

    private final ElasticJob elasticJob;

    public SpringJobSchedulerEnhance(final ElasticJob elasticJob, final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration jobConfig, final ElasticJobListener... elasticJobListeners) {
        super(regCenter, jobConfig, getTargetElasticJobListeners(elasticJobListeners));
        this.elasticJob = elasticJob;
    }

    public SpringJobSchedulerEnhance(final ElasticJob elasticJob, final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration jobConfig,
                                     final JobEventConfiguration jobEventConfig, final ElasticJobListener... elasticJobListeners) {
        super(regCenter, jobConfig, jobEventConfig, getTargetElasticJobListeners(elasticJobListeners));
        this.elasticJob = elasticJob;
    }

    private static ElasticJobListener[] getTargetElasticJobListeners(final ElasticJobListener[] elasticJobListeners) {
        final ElasticJobListener[] result = new ElasticJobListener[elasticJobListeners.length];
        for (int i = 0; i < elasticJobListeners.length; i++) {
            result[i] = (ElasticJobListener) AopTargetUtils.getTarget(elasticJobListeners[i]);
        }
        return result;
    }

    public static SpringJobSchedulerEnhance load(String jobName, final ElasticJob elasticJob, final CoordinatorRegistryCenter regCenter, final ElasticJobListener... elasticJobListeners) {
        ConfigurationService configService = new ConfigurationService(regCenter, jobName);
        LiteJobConfiguration liteJobConfigFromRegCenter = configService.load(false);
        if (null != liteJobConfigFromRegCenter) {
            SpringJobSchedulerEnhance springJobSchedulerEnhance = new SpringJobSchedulerEnhance(elasticJob, regCenter, liteJobConfigFromRegCenter, getTargetElasticJobListeners(elasticJobListeners));
            JobRegistry.getInstance().setCurrentShardingTotalCount(liteJobConfigFromRegCenter.getJobName(), liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getShardingTotalCount());
            JobScheduleController jobScheduleController = new JobScheduleController(
                    (Scheduler) ReflectionUtils.invokeMethod(createScheduler, springJobSchedulerEnhance),
                    (JobDetail) ReflectionUtils.invokeMethod(createJobDetail, springJobSchedulerEnhance, liteJobConfigFromRegCenter.getTypeConfig().getJobClass()),
                    liteJobConfigFromRegCenter.getJobName()
            );
            JobRegistry.getInstance().registerJob(liteJobConfigFromRegCenter.getJobName(), jobScheduleController, regCenter);
            springJobSchedulerEnhance.getSchedulerFacade().registerStartUpInfo(!liteJobConfigFromRegCenter.isDisabled());
            jobScheduleController.scheduleJob(liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getCron()); //com.dangdang.ddframe.job.exception.JobSystemException: org.quartz.SchedulerException: Based on configured schedule, the given trigger 'DEFAULT.javaSimpleJob3' will never fire.
            return springJobSchedulerEnhance;
        } else {
            // 空目录
            regCenter.remove("/" + jobName);
            return null;
        }
    }

    public static Boolean tryTriggerOnce(String jobName) {
        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
//        jobScheduleController.triggerJob();//不一定能触发
        if (null != jobScheduleController) {
            JobDetail jobDetail = (JobDetail) ReflectionUtils.getField(jobDetailField, jobScheduleController);
            LiteJobFacade liteJobFacade = ((LiteJobFacade) jobDetail.getJobDataMap().get("jobFacade"));
            ShardingContexts shardingContexts = liteJobFacade.getShardingContexts();
            //目前只支持SimpleJob
            ShardingContext shardingContext = new ShardingContext(shardingContexts, 0);
            SimpleJob elasticJob = (SimpleJob) jobDetail.getJobDataMap().get("elasticJob");
            elasticJob.execute(shardingContext);
            //后置流程
            liteJobFacade.afterJobExecuted(shardingContexts);
            return true;
        } else {
            return false;
        }
    }


    @Override
    protected Optional<ElasticJob> createElasticJobInstance() {
        return Optional.fromNullable(elasticJob);
    }
}
