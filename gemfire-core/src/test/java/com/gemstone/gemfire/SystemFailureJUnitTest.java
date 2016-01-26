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
package com.gemstone.gemfire;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SystemFailureJUnitTest {

  private static final int LONG_WAIT = 30000;
  private int oldWaitTime;
  @Before
  public void setWaitTime() {
    oldWaitTime = SystemFailure.SHUTDOWN_WAIT;
    SystemFailure.SHUTDOWN_WAIT = LONG_WAIT;
  }
  
  @After
  public void restoreWaitTime() {
    SystemFailure.SHUTDOWN_WAIT = oldWaitTime;
  }
  @Test
  public void testStopThreads() {
    SystemFailure.startThreads();
    long start = System.nanoTime();
    Thread watchDog = SystemFailure.getWatchDogForTest();
    Thread proctor= SystemFailure.getProctorForTest();
    assertTrue(watchDog.isAlive());
    assertTrue(proctor.isAlive());
    SystemFailure.stopThreads();
    long elapsed = System.nanoTime() - start;
    assertTrue("Waited too long to shutdown: " + elapsed, elapsed < TimeUnit.MILLISECONDS.toNanos(LONG_WAIT));
    assertFalse(watchDog.isAlive());
    assertFalse(proctor.isAlive());
  }

}
