/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks flushes using a queue of latches.
 * 
 * @author bakera
 */
public class SignalledFlushObserver implements FlushObserver {
  private static class FlushLatch extends CountDownLatch {
    private final long seqnum;
    
    public FlushLatch(long seqnum) {
      super(1);
      this.seqnum = seqnum;
    }
    
    public long getSequence() {
      return seqnum;
    }
  }
  
  // assume the number of outstanding flush requests is small so we don't
  // need to organize by seqnum
  private final List<FlushLatch> signals;
  
  private final AtomicLong eventsReceived;
  private final AtomicLong eventsDelivered;
  
  public SignalledFlushObserver() {
    signals = new ArrayList<FlushLatch>();
    eventsReceived = new AtomicLong(0);
    eventsDelivered = new AtomicLong(0);
  }
  
  @Override
  public boolean shouldDrainImmediately() {
    synchronized (signals) {
      return !signals.isEmpty();
    }
  }
  
  @Override
  public AsyncFlushResult flush() {
    final long seqnum = eventsReceived.get();
    synchronized (signals) {
      final FlushLatch flush;
      if (seqnum <= eventsDelivered.get()) {
        flush = null;
      } else {
        flush = new FlushLatch(seqnum);
        signals.add(flush);
      }
      
      return new AsyncFlushResult() {
        @Override
        public boolean waitForFlush(long timeout, TimeUnit unit) throws InterruptedException {
          return flush == null ? true : flush.await(timeout, unit);
        }
      };
    }
  }

  /**
   * Invoked when an event is received.
   */
  public void push() {
    eventsReceived.incrementAndGet();
  }

  /**
   * Invoked when a batch has been dispatched.
   */
  public void pop(int count) {
    long highmark = eventsDelivered.addAndGet(count);
    synchronized (signals) {
      for (ListIterator<FlushLatch> iter = signals.listIterator(); iter.hasNext(); ) {
        FlushLatch flush = iter.next();
        if (flush.getSequence() <= highmark) {
          flush.countDown();
          iter.remove();
        }
      }
    }
  }
  
  /**
   * Invoked when the queue is cleared.
   */
  public void clear() {
    synchronized (signals) {
      for (FlushLatch flush : signals) {
        flush.countDown();
      }

      signals.clear();
      eventsReceived.set(0);
      eventsDelivered.set(0);
    }
  }
}
