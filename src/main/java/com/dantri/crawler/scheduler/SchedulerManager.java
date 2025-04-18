package com.dantri.crawler.scheduler;

import com.dantri.crawler.job.CrawlJob;
import com.dantri.crawler.queue.CrawlQueueManager;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class SchedulerManager {
    private static final Logger log = LoggerFactory.getLogger(SchedulerManager.class);
    private Scheduler scheduler;

    public void start(CrawlQueueManager q) throws SchedulerException {
        scheduler = StdSchedulerFactory.getDefaultScheduler();

        // Gọi CrawlJob.execute()
        JobDetail job = newJob(CrawlJob.class)
                .withIdentity("crawlJob","group")
                .build();
        job.getJobDataMap().put("queueManager", q);

        Trigger trg = newTrigger()
                .withIdentity("crawlTrigger","group")
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMinutes(5) // Lập lịch 5 phút
                        .repeatForever())
                .build();

        scheduler.scheduleJob(job, trg);
        scheduler.start();
        log.info("Scheduler started");
    }

    public void shutdown() throws SchedulerException {
        if (scheduler!=null) scheduler.shutdown();
    }
}
