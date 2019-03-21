package org.apache.geode.cache.client.internal.pooling;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for counting connections. Transient refactoring to towards cleaner implementation of
 * {@link ConnectionManager}.
 *
 */
public class ConnectionAccounting {
  private final int min;
  private final int max;
  private final AtomicInteger count = new AtomicInteger();

  public ConnectionAccounting(int min, int max) {
    this.min = min;
    this.max = max;
  }

  public int getMin() {
    return min;
  }

  public int getMax() {
    return max;
  }

  public int getCount() {
    return count.get();
  }

  /**
   * Should be called why a new connection would be nice to have when count is under max. Caller should only
   * create a connection if this method returns {@code true}. If connection creation fails then {@link #cancelTryCreate} must
   * be called to revert the count increase.
   *
   * @return {@code true} if count was under max and we increased it, otherwise {@code false}.
   */
  public boolean tryCreate() {
    int currentCount;
    while ((currentCount = count.get()) < max) {
      if (count.compareAndSet(currentCount, currentCount + 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Should only be called if connection creation failed after calling {@link #tryCreate()}.
   */
  public void cancelTryCreate() {
    count.getAndDecrement();
  }

  /**
   * Count a created connection regardless of max. Should not be called after {@link #tryCreate()}.
   */
  public void create() {
    count.getAndIncrement();
  }

  /**
   * Should be called when a connection is being returned and the caller should destroy the connection if
   * {@code true} was returned. If connection destroy fails then {@link #cancelTryDestroy()} must be called.
   *
   * @return {@code true} if count was over max and we decreased it, otherwise {@code false}.
   */
  public boolean tryDestroy() {
    int currentCount;
    while ((currentCount = count.get()) > max) {
      if (count.compareAndSet(currentCount, currentCount - 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Should only be called if connection destroy failed after calling {@link #tryDestroy()}.
   */
  public void cancelTryDestroy() {
    count.getAndIncrement();
  }

  /**
   * Should be called after any connection destroy done regardless of max. Should not be called after {@link #tryDestroy()}.
   *
   * @param destroyCount number of connections being destroyed.
   *
   * @return {@code true} if after decreasing count it is under the minimum, otherwise {@code false}.
   */
  public boolean destroyAndIsUnderMinimum(int destroyCount) {
    return count.addAndGet(-destroyCount) < min;
  }

  public boolean isUnderMinimum() {
    return count.get() < min;
  }

  public boolean isOverMinimum() {
    return count.get() > min;
  }
}
