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
    private Scheduler sched;

    public void start(CrawlQueueManager q) throws SchedulerException {
        sched = StdSchedulerFactory.getDefaultScheduler();

        JobDetail job = newJob(CrawlJob.class)
                .withIdentity("crawlJob","grp")
                .build();
        job.getJobDataMap().put("queueManager", q);

        Trigger trg = newTrigger()
                .withIdentity("crawlTrigger","grp")
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMinutes(5)
                        .repeatForever())
                .build();

        sched.scheduleJob(job, trg);
        sched.start();
        log.info("Scheduler started");
    }

    public void shutdown() throws SchedulerException {
        if (sched!=null) sched.shutdown();
    }
}
