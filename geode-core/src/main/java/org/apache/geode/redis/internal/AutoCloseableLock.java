package org.apache.geode.redis.internal;

import org.apache.geode.cache.Region;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/**
 * Created by gosullivan on 3/10/17.
 */
public class AutoCloseableLock implements AutoCloseable {
  Runnable unlock;

  public AutoCloseableLock(Runnable unlockFunction) {
    unlock = unlockFunction;
  }

  @Override
  public void close() {
    this.unlock.run();
  }
}
