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
package org.apache.geode.internal.process;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.apache.geode.internal.process.ProcessStreamReader.ReadingMode.BLOCKING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Functional integration test {@link #hangsOnWindows} for BlockingProcessStreamReader which
 * verifies TRAC #51967: "GFSH start hangs on Windows." The hang is supposedly caused by a JDK bug
 * in which a thread invoking readLine() will block forever and ignore any interrupts. The thread
 * will respond to interrupts as expected on Mac, Linux and Solaris.
 *
 * @see BlockingProcessStreamReaderIntegrationTest
 * @see NonBlockingProcessStreamReaderIntegrationTest
 *
 * @since GemFire 8.2
 */
public class BlockingProcessStreamReaderWindowsTest
    extends AbstractProcessStreamReaderIntegrationTest {

  /** Timeout to confirm hang on Windows */
  private static final int HANG_TIMEOUT_SECONDS = 10;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() throws Exception {
    assumeTrue(isWindows());
  }

  @Test
  public void hangsOnWindows() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    Future<Boolean> future = executorServiceRule.submit(() -> {
      process.getOutputStream().close();
      process.getErrorStream().close();
      process.getInputStream().close();
      return true;
    });

    // assert
    assertThatThrownBy(() -> future.get(HANG_TIMEOUT_SECONDS, SECONDS))
        .isInstanceOf(TimeoutException.class);
  }

  @Override
  protected ReadingMode getReadingMode() {
    return BLOCKING;
  }
}
