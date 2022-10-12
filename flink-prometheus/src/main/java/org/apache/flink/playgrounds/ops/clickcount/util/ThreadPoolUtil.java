package org.apache.flink.playgrounds.ops.clickcount.util;

import java.util.concurrent.*;

public class ThreadPoolUtil {
    // 直接上饿汉单例
    private static ExecutorService es = Executors.newCachedThreadPool();

    public static ExecutorService getInstance(){
        return es;
    }

}
