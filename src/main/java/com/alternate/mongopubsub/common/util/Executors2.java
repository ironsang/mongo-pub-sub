package com.alternate.mongopubsub.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Executors2 {
    public static ExecutorService newRetrySingleThreadExecutor(int maxRetries) {
        return new RetryExecutor(1, 1, maxRetries,0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }
}
