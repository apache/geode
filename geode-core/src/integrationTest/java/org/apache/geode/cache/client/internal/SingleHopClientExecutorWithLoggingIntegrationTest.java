/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.client.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Test if exceptions are logged when thread is submitted using
 * {@code SingleHopClientExecutor#submitTask} method.
 */
@Category({ClientServerTest.class, LoggingTest.class})
public class SingleHopClientExecutorWithLoggingIntegrationTest {

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  /**
   * Refer: GEODE-2109 This test verifies that any exception thrown from forked thread is logged
   * into log.
   */
  @Test
  @Ignore("Until GEODE-5637 is fixed")
  public void submittedTaskShouldLogFailure() {
    String message = "I am expecting this to be logged";

    SingleHopClientExecutor.submitTask(() -> {
      // test piece throwing exception
      throw new RuntimeException(message);
    });

    /*
     * Sometimes need to wait for more than sec as thread execution takes time.
     */
    await()
        .untilAsserted(() -> assertThat(systemErrRule.getLog()).contains(message));
  }
}
