package org.apache.flink.playgrounds.ops.clickcount.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.playgrounds.ops.clickcount.util.DimUtil;
import org.apache.flink.playgrounds.ops.clickcount.util.ThreadPoolUtil;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;


import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class DimAsyncFunction extends RichAsyncFunction<ClickEvent,ClickEvent>{

    private ExecutorService executorService;

    private Counter hitCounter = null;
    private Counter totalCounter = null;

    private ReentrantLock hitLock = null;
    private ReentrantLock totalLock = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 自定指标组名，计数器名称：
        // flink_taskmanager_job_task_operator_CacheHit_hitCounter
        hitCounter = getRuntimeContext().getMetricGroup().addGroup("CacheHit").counter("hitCounter");
        // flink_taskmanager_job_task_operator_CacheHit_totalCounter
        totalCounter = getRuntimeContext().getMetricGroup().addGroup("CacheHit").counter("totalCounter");
        executorService = ThreadPoolUtil.getInstance();
        hitLock = new ReentrantLock();
        totalLock = new ReentrantLock();
    }

    @Override
    public void asyncInvoke(ClickEvent event, ResultFuture<ClickEvent> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                String page = event.getPage();
                Tuple2<String, Boolean> result = DimUtil.query(page);
                event.setType(result.f0);
                resultFuture.complete(Collections.singleton(event));

                // 由于使用了线程池，涉及多线程操作共享资源问题，flink官方提供的Counter是线程不安全的。因此需要加锁实现多线程串行累计操作
                if(result.f1){
                    try{
                        hitLock.lock();
                        hitCounter.inc();
                    }finally {
                        hitLock.unlock();
                    }
                }

                try{
                    totalLock.lock();
                    totalCounter.inc();
                }finally {
                    totalLock.unlock();
                }
            }
        });
    }

}
