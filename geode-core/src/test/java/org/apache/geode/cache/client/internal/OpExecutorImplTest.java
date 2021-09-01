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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.net.SocketTimeoutException;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;

public class OpExecutorImplTest {

  @Test
  public void handleExceptionWithSocketTimeoutExceptionDoesNotThrowException() {
    assertThatNoException()
        .isThrownBy(() -> OpExecutorImpl.handleException(new SocketTimeoutException(),
            mock(Connection.class), 0, false,
            false, mock(CancelCriterion.class), mock(EndpointManager.class)));
  }

  @Test
  public void handleExceptionWithSocketTimeoutExceptionAndFinalAttemptThrowsServerConnectivityExceptionWithoutCause() {
    assertThatThrownBy(() -> OpExecutorImpl.handleException(new SocketTimeoutException(),
        mock(Connection.class), 0, true,
        false, mock(CancelCriterion.class), mock(EndpointManager.class)))
            .isInstanceOf(ServerConnectivityException.class).hasNoCause();
  }

  @Test
  public void handleExceptionWithConnectionDestroyedExceptionDoesNotThrowException() {
    assertThatNoException()
        .isThrownBy(() -> OpExecutorImpl.handleException(new ConnectionDestroyedException(),
            mock(Connection.class), 0, false,
            false, mock(CancelCriterion.class), mock(EndpointManager.class)));
  }

  @Test
  public void handleExceptionWithConnectionDestroyedExceptionAndFinalAttemptThrowsServerConnectivityExceptionWithoutCause() {
    assertThatThrownBy(() -> OpExecutorImpl.handleException(new ConnectionDestroyedException(),
        mock(Connection.class), 0, true,
        false, mock(CancelCriterion.class), mock(EndpointManager.class)))
            .isInstanceOf(ServerConnectivityException.class).hasNoCause();
  }

}
