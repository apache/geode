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
package org.apache.geode.internal.monitoring.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class P2PReaderExecutorGroupTest {

  @Test
  public void testVerifyGroupName() {
    assertThat(new P2PReaderExecutorGroup().getGroupName())
        .isEqualTo(P2PReaderExecutorGroup.GROUP_NAME);
  }

  @Test
  public void verifySuspendLifecycle() {
    P2PReaderExecutorGroup executor = new P2PReaderExecutorGroup();
    assertThat(executor.isMonitoringSuspended()).isFalse();
    executor.suspendMonitoring();
    assertThat(executor.isMonitoringSuspended()).isTrue();
    executor.suspendMonitoring();
    assertThat(executor.isMonitoringSuspended()).isTrue();
    executor.resumeMonitoring();
    assertThat(executor.isMonitoringSuspended()).isFalse();
    executor.resumeMonitoring();
    assertThat(executor.isMonitoringSuspended()).isFalse();
    executor.suspendMonitoring();
    assertThat(executor.isMonitoringSuspended()).isTrue();
  }

  @Test
  public void verifyResumeClearsStartTime() {
    P2PReaderExecutorGroup executor = new P2PReaderExecutorGroup();
    executor.setStartTime(1);
    assertThat(executor.getStartTime()).isEqualTo(1);
    executor.resumeMonitoring();
    assertThat(executor.getStartTime()).isEqualTo(0);
  }
}
