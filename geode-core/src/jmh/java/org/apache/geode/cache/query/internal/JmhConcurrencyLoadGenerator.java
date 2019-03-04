package org.apache.geode.cache.query.internal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JmhConcurrencyLoadGenerator {
  final ScheduledThreadPoolExecutor loadGenerationExecutorService;
  private final int numThreads;
  public JmhConcurrencyLoadGenerator(int numThreads) {

    this.numThreads = numThreads;
    loadGenerationExecutorService =
        (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
            numThreads);
    System.out.println(String.format("Pool has %d threads", numThreads));
    loadGenerationExecutorService.setRemoveOnCancelPolicy(true);

  }

  public void generateLoad(int delay, TimeUnit timeUnit, Runnable runnable) {
    for (int i = 0; i < numThreads; i++) {
      loadGenerationExecutorService.schedule(runnable, delay, timeUnit);
    }
  }

  public void tearDown() {
    loadGenerationExecutorService.shutdownNow();
  }
}