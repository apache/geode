package org.apache.geode.cache.client.internal.pooling;

public class NotAtomicInteger {
  int value = 0;

  public int get() {
    return value;
  }

  public boolean compareAndSet(int currentCount, int i) {
    if (value == currentCount) {
      value = i;
      return true;
    }

    return false;
  }

  public int getAndDecrement() {
    int current = value;
    value--;
    return current;
  }

  public int getAndIncrement() {
    int current = value;
    value--;
    return current;
  }

  public int addAndGet(int i) {
    value+=i;
    return value;
  }
}
