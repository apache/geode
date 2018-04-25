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
package org.apache.geode.cache.client.internal;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Test if exceptions are logged when thread is submitted using
 * {@code SingleHopClientExecutor#submitTask} method.
 */
@Category({UnitTest.class, ClientServerTest.class})
public class SingleHopClientExecutorSubmitTaskWithExceptionTest {

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  /**
   * Refer: GEODE-2109 This test verifies that any exception thrown from forked thread is logged
   * into log.
   */
  @Test
  public void submittedTaskShouldLogFailure() {
    String erroMsg = "I am expecting this to be logged";

    SingleHopClientExecutor.submitTask(new Runnable() {
      @Override
      public void run() {
        // test piece throwing exception
        throw new RuntimeException(erroMsg);
      }
    });

    /**
     * Sometimes need to wait for more than sec as thread execution takes time.
     */
    Awaitility.await("Waiting for exception").atMost(60l, TimeUnit.SECONDS).until(() -> {
      systemErrRule.getLog().contains(erroMsg);
    });
  }

}
