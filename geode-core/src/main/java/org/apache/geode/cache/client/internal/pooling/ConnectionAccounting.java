package org.apache.geode.cache.client.internal.pooling;

import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionAccounting {
  private final int min;
  private final int max;
  private final AtomicInteger count = new AtomicInteger();

  public ConnectionAccounting(int min, int max) {
    this.min = min;
    this.max = max;
  }

  public int decrementAndGetConnectionCount() {
    return decrementAndGetConnectionCount(1);
  }

  public boolean tryReserveConnection() {
    int currentConnectionCount;
    while ((currentConnectionCount = count.get()) < max) {
      if (count.compareAndSet(currentConnectionCount, currentConnectionCount + 1)) {
        return true;
      }
    }
    return false;
  }

  public void incrementConnectionCount() {
    count.getAndIncrement();
  }

  public boolean shouldDestroy() {
    int currentCount;
    while ((currentCount = count.get()) > max) {
      if (count.compareAndSet(currentCount, currentCount - 1)) {
        return true;
      }
    }
    return false;
  }

  public boolean isUnderMinimum() {
    return count.get() < min;
  }

  public int getConnectionCount() {
    return count.get();
  }

  private int decrementAndGetConnectionCount(int decrementBy) {
    return count.addAndGet(-decrementBy);
  }

  public int getMinConnections() {
    return this.min;
  }

  public int getMaxConnections() {
    return this.max;
  }

  public boolean decrementAndIsUnderMinimum() {
    return decrementAndIsUnderMinimum(1);
  }

  public boolean decrementAndIsUnderMinimum(int decrementBy) {
    return decrementAndGetConnectionCount(decrementBy) < min;
  }

  public boolean isOverMinimum() {
    return count.get() > min;
  }
}
