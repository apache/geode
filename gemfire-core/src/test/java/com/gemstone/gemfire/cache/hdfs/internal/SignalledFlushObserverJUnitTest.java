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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.internal.FlushObserver.AsyncFlushResult;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

import junit.framework.TestCase;

@Category({IntegrationTest.class})
public class SignalledFlushObserverJUnitTest extends TestCase {
  private AtomicInteger events;
  private AtomicInteger delivered;
  
  private SignalledFlushObserver sfo;
  
  public void testEmpty() throws InterruptedException {
    assertFalse(sfo.shouldDrainImmediately());
    assertTrue(sfo.flush().waitForFlush(0, TimeUnit.NANOSECONDS));
    assertFalse(sfo.shouldDrainImmediately());
  }
  
  public void testSingle() throws InterruptedException {
    sfo.push();
    AsyncFlushResult result = sfo.flush();

    assertTrue(sfo.shouldDrainImmediately());
    sfo.pop(1);
    
    assertTrue(result.waitForFlush(0, TimeUnit.MILLISECONDS));
    assertFalse(sfo.shouldDrainImmediately());
  }

  public void testDouble() throws InterruptedException {
    sfo.push();
    sfo.push();

    AsyncFlushResult result = sfo.flush();
    assertTrue(sfo.shouldDrainImmediately());

    sfo.pop(1);
    assertFalse(result.waitForFlush(0, TimeUnit.MILLISECONDS));

    sfo.pop(1);
    assertTrue(result.waitForFlush(0, TimeUnit.MILLISECONDS));
    assertFalse(sfo.shouldDrainImmediately());
  }

  public void testTimeout() throws InterruptedException {
    sfo.push();
    AsyncFlushResult result = sfo.flush();

    assertTrue(sfo.shouldDrainImmediately());
    assertFalse(result.waitForFlush(100, TimeUnit.MILLISECONDS));
    sfo.pop(1);
    
    assertTrue(result.waitForFlush(0, TimeUnit.MILLISECONDS));
    assertFalse(sfo.shouldDrainImmediately());
  }
  
  @Override
  protected void setUp() {
    events = new AtomicInteger(0);
    delivered = new AtomicInteger(0);
    sfo = new SignalledFlushObserver();
    AbstractHoplogOrganizer.JUNIT_TEST_RUN = true;
  }
  
  private int push() {
    return events.incrementAndGet();
  }
  
  private int pop() {
    return delivered.incrementAndGet();
  }
}
