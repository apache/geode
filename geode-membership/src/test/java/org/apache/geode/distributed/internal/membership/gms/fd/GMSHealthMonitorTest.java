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
package org.apache.geode.distributed.internal.membership.gms.fd;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreatorImpl;

public class GMSHealthMonitorTest {

  @Test
  public void throwRejectedExecutionExceptionIfMonitorIsNotStopping() {
    final GMSHealthMonitor<MemberIdentifier> monitor =
        new GMSHealthMonitor<>(new TcpSocketCreatorImpl());
    assertThatThrownBy(() -> {
      monitor.doAndIgnoreRejectedExecutionExceptionIfStopping(() -> {
        throw new RejectedExecutionException();
      });
    }).isInstanceOf(RejectedExecutionException.class);
  }

  @Test
  public void doNotThrowRejectedExecutionExceptionIfMonitorIsStopping() {
    final GMSHealthMonitor<MemberIdentifier> monitor =
        new GMSHealthMonitor<>(new TcpSocketCreatorImpl());
    monitor.stop();
    assertThatNoException().isThrownBy(() -> {
      monitor.doAndIgnoreRejectedExecutionExceptionIfStopping(() -> {
        throw new RejectedExecutionException();
      });
    });
  }

  @Test
  public void throwOtherExceptionIfMonitorIsStopping() {
    final GMSHealthMonitor<MemberIdentifier> monitor =
        new GMSHealthMonitor<>(new TcpSocketCreatorImpl());
    monitor.stop();
    assertThatThrownBy(() -> {
      monitor.doAndIgnoreRejectedExecutionExceptionIfStopping(() -> {
        throw new RuntimeException();
      });
    }).isInstanceOf(RuntimeException.class);
  }
}
