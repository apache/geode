/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.distributed.internal;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * An instance of ThrottlingMemLinkedQueue allows the instantiator to specify a maximum queue
 * footprint (M) and a size to begin throttling (B) (which must be between 1 and the M). When adding
 * an element to the queue, if the size of the queue is less than B, the element is added
 * immediately. In case of udp, If the size of the queue has reached M, the add will block until the
 * size is less than M. If the size of the queue is between B and M, the add will block with a sleep
 * time that is at least 1 millisecond, and is proportional to the size of the queue.
 *
 * ThrottlingMemLinkedQueue objects can currently hold only Sizeable objects. Inserting other types
 * of objects will cause class cast exceptions to be thrown on put/take.
 *
 * @since GemFire 3.0
 *
 */

public class ThrottlingMemLinkedQueueWithDMStats<E> extends OverflowQueueWithDMStats<E> {
  private static final long serialVersionUID = 5425180246954573433L;

  /** The maximum size of the queue */
  private final int maxMemSize;

  /** The size at which to beging throttling */
  private final int startThrottleMemSize;

  /** The maximum size of the queue */
  private final int maxSize;

  /** The size at which to begin throttling */
  private final int startThrottleSize;

  /** The current memory footprint of the queue */
  private final AtomicInteger memSize = new AtomicInteger();

  /** Creates a new instance of ThrottlingMessageQueue */
  public ThrottlingMemLinkedQueueWithDMStats(int maxMemSize, int startThrottleMemSize, int maxSize,
      int startThrottleSize, ThrottledMemQueueStatHelper stats) {
    super(maxSize, stats);
    this.maxMemSize = maxMemSize;
    this.startThrottleMemSize = startThrottleMemSize;
    this.maxSize = maxSize;
    this.startThrottleSize = startThrottleSize;
  }

  private int calculateThrottleTime() {
    int sleep;

    int myMemSize = memSize.get();
    if (myMemSize > startThrottleMemSize) {
      sleep = (int) (((float) (myMemSize - startThrottleMemSize)
          / (float) (maxMemSize - startThrottleMemSize)) * 100);
    } else {
      int qSize = size();
      if (qSize > startThrottleSize) {
        sleep = (int) (((float) (qSize - startThrottleSize) / (float) (maxSize - startThrottleSize))
            * 100);
      } else {
        // no need to throttle
        return 0;
      }
    }

    // Increment sleep count with linear step as the size approaches max value.
    sleep = sleep * ((sleep / 10) + 1);
    sleep = Math.max(sleep, 1);

    return sleep;
  }

  @Override
  protected void preAdd(Object o) {
    try {
      preAddInterruptibly(o);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void preAddInterruptibly(Object o) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    // only block threads reading from tcp stream sockets. blocking udp
    // will cause retransmission storms
    if (!DistributionMessage.isMembershipMessengerThread()) {
      long startTime = DistributionStats.getStatTime();
      do {
        try {
          int sleep = calculateThrottleTime();
          if (sleep > 0) {
            Thread.sleep(sleep);
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          // The interrupt terminates the throttling sleep and quickly
          // returns, which is probably the Right Thing.
        }
        if (DistributionStats.enableClockStats) {
          final long endTime = DistributionStats.getStatTime();
          ((ThrottledMemQueueStatHelper) this.stats).throttleTime(endTime - startTime);
          startTime = endTime;
        }
      } while (memSize.get() >= maxMemSize || size() >= maxSize);

      ((ThrottledMemQueueStatHelper) this.stats).incThrottleCount();
    }

    if (o instanceof Sizeable) {
      int mem = ((Sizeable) o).getSize();
      ((ThrottledMemQueueStatHelper) this.stats).addMem(mem);
      this.memSize.addAndGet(mem);
    }
  }

  @Override
  protected void postRemove(Object o) {
    if (o instanceof Sizeable) {
      int mem = ((Sizeable) o).getSize();
      this.memSize.addAndGet(-mem);
      ((ThrottledMemQueueStatHelper) this.stats).removeMem(mem);
    }
  }

  @Override
  protected void postDrain(Collection c) {
    for (Object aC : c) {
      postRemove(aC);
    }
  }
}
