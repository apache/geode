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
package org.apache.geode.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.Thread.UncaughtExceptionHandler;

import org.junit.Test;

public class LoggingThreadTest {

  @Test
  public void verifyCreateSetsNameOnThread() {
    Thread thread = new LoggingThread("loggingThreadName", false, null);

    assertThat(thread.getName()).isEqualTo("loggingThreadName");
  }

  @Test
  public void verifyCreateReturnsNonDaemon() {
    Thread thread = new LoggingThread("loggingThreadName", false, null);

    assertThat(thread.isDaemon()).isFalse();
  }

  @Test
  public void verifyCreateSetExpectedHandler() {
    UncaughtExceptionHandler handler = LoggingUncaughtExceptionHandler.getInstance();

    Thread thread = new LoggingThread("loggingThreadName", false, null);

    assertThat(thread.getUncaughtExceptionHandler()).isSameAs(handler);
  }

  @Test
  public void verifyCreateDaemonSetsNameOnThread() {
    Thread thread = new LoggingThread("loggingThreadName", null);

    assertThat(thread.getName()).isEqualTo("loggingThreadName");
  }

  @Test
  public void verifyCreateDaemonReturnsDaemon() {
    Thread thread = new LoggingThread("loggingThreadName", null);

    assertThat(thread.isDaemon()).isTrue();
  }

  @Test
  public void verifyCreateDaemonSetExpectedHandler() {
    UncaughtExceptionHandler handler = LoggingUncaughtExceptionHandler.getInstance();

    Thread thread = new LoggingThread("loggingThreadName", null);

    assertThat(thread.getUncaughtExceptionHandler()).isSameAs(handler);
  }

}
