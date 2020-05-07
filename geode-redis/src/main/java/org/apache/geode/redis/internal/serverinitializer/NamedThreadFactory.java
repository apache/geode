package org.apache.geode.redis.internal.serverinitializer;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
  private final AtomicInteger counter = new AtomicInteger();
  private final String baseName;
  private final boolean isDaemon;

  public NamedThreadFactory(String baseName, boolean isDaemon) {
    this.baseName = baseName;
    this.isDaemon = isDaemon;
  }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setName(baseName + counter.incrementAndGet());
      thread.setDaemon(isDaemon);
      return thread;
    }
}
