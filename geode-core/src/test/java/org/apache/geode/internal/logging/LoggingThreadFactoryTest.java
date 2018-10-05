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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.Thread.UncaughtExceptionHandler;

import org.junit.Test;

import org.apache.geode.internal.logging.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.internal.logging.LoggingThreadFactory.ThreadInitializer;

public class LoggingThreadFactoryTest {

  @Test
  public void verifyFirstThreadName() {
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName");

    Thread thread = factory.newThread(null);

    assertThat(thread.getName()).isEqualTo("baseName" + 1);
  }

  @Test
  public void verifySecondThreadName() {
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName");
    factory.newThread(null);

    Thread thread = factory.newThread(null);

    assertThat(thread.getName()).isEqualTo("baseName" + 2);
  }

  @Test
  public void verifyThreadsAreDaemons() {
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName");

    Thread thread = factory.newThread(null);

    assertThat(thread.isDaemon()).isTrue();
  }

  @Test
  public void verifyThreadHaveExpectedHandler() {
    UncaughtExceptionHandler handler = LoggingUncaughtExceptionHandler.getInstance();
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName");

    Thread thread = factory.newThread(null);

    assertThat(thread.getUncaughtExceptionHandler()).isSameAs(handler);
  }

  @Test
  public void verifyThreadInitializerCalledCorrectly() {
    ThreadInitializer threadInitializer = mock(ThreadInitializer.class);
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName", threadInitializer, null);

    Thread thread = factory.newThread(null);

    verify(threadInitializer).initialize(thread);
  }

  @Test
  public void verifyCommandWrapperNotCalledIfThreadIsNotStarted() {
    CommandWrapper commandWrapper = mock(CommandWrapper.class);
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName", commandWrapper);

    Thread thread = factory.newThread(null);

    verify(commandWrapper, never()).invoke(any());
  }

  @Test
  public void verifyCommandWrapperCalledIfThreadStarted() throws InterruptedException {
    CommandWrapper commandWrapper = mock(CommandWrapper.class);
    Runnable command = mock(Runnable.class);
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName", commandWrapper);

    Thread thread = factory.newThread(command);
    thread.start();
    thread.join();

    verify(commandWrapper).invoke(command);
  }

  @Test
  public void verifyCommandCalledIfThreadStarted() throws InterruptedException {
    Runnable command = mock(Runnable.class);
    LoggingThreadFactory factory = new LoggingThreadFactory("baseName");

    Thread thread = factory.newThread(command);
    thread.start();
    thread.join();

    verify(command).run();
  }
}
