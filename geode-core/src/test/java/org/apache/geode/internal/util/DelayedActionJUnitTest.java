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
package org.apache.geode.internal.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;


public class DelayedActionJUnitTest {

  @Test
  public void testDelay() throws InterruptedException {
    final AtomicBoolean hit = new AtomicBoolean(false);
    final CountDownLatch complete = new CountDownLatch(1);

    Runnable r = () -> {
      hit.set(true);
      complete.countDown();
    };

    DelayedAction delay = new DelayedAction(r);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    exec.execute(delay);

    delay.waitForArrival();
    assertFalse(hit.get());

    delay.allowToProceed();
    complete.await();
    assertTrue(hit.get());
  }
}
