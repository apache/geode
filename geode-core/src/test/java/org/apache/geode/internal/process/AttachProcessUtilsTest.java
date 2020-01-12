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

import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.process.lang.AvailablePid;
import org.apache.geode.test.junit.Retry;
import org.apache.geode.test.junit.rules.RetryRule;

/**
 * Unit tests for {@link AttachProcessUtils}.
 *
 * <p>
 * Tests involving fakePid use {@link RetryRule} because the fakePid may become used by a real
 * process before the test executes.
 */
public class AttachProcessUtilsTest {

  private static final int PREFERRED_FAKE_PID = 42;

  private int actualPid;
  private int fakePid;
  private AttachProcessUtils attachProcessUtils;

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void before() throws Exception {
    actualPid = identifyPid();
    fakePid = new AvailablePid().findAvailablePid(PREFERRED_FAKE_PID);
    attachProcessUtils = new AttachProcessUtils();
  }

  @Test
  public void isAttachApiAvailable_returnsTrue() throws Exception {
    assertThat(attachProcessUtils.isAttachApiAvailable()).isTrue();
  }

  @Test
  public void isAvailable_returnsTrue() throws Exception {
    assertThat(attachProcessUtils.isAvailable()).isTrue();
  }

  @Test
  public void isProcessAlive_withLivePid_returnsTrue() throws Exception {
    assertThat(attachProcessUtils.isProcessAlive(actualPid)).isTrue();
  }

  @Test
  @Retry(3)
  public void isProcessAlive_withDeadPid_returnsFalse() throws Exception {
    assertThat(attachProcessUtils.isProcessAlive(fakePid)).isFalse();
  }

  @Test
  @Retry(3)
  public void killProcess_throwsUnsupportedOperationException() throws Exception {
    assertThatThrownBy(() -> attachProcessUtils.killProcess(fakePid))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("killProcess(int) not supported by AttachProcessUtils");
  }
}
