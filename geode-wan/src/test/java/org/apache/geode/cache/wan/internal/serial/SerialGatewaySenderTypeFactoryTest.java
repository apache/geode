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

package org.apache.geode.cache.wan.internal.serial;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MutableGatewaySenderAttributes;

public class SerialGatewaySenderTypeFactoryTest {

  private final MutableGatewaySenderAttributes attributes =
      mock(MutableGatewaySenderAttributes.class);

  private final SerialGatewaySenderTypeFactory factory = new SerialGatewaySenderTypeFactory();

  @Test
  public void validateThrowsIfAsyncEventListenersAdded() {
    final List<AsyncEventListener> asyncEventListeners = uncheckedCast(mock(List.class));
    when(asyncEventListeners.isEmpty()).thenReturn(false);
    when(attributes.getAsyncEventListeners()).thenReturn(asyncEventListeners);

    assertThatThrownBy(() -> factory.validate(attributes))
        .isInstanceOf(GatewaySenderException.class).hasMessageContaining(
            "cannot define a remote site because at least AsyncEventListener is already added");
  }

  @Test
  public void validateThrowsIfMustGroupTransactionEventsAndDispatcherThreadsGreaterThan1() {
    when(attributes.mustGroupTransactionEvents()).thenReturn(true);
    when(attributes.getDispatcherThreads()).thenReturn(2);

    assertThatThrownBy(() -> factory.validate(attributes))
        .isInstanceOf(GatewaySenderException.class).hasMessageContaining(
            "cannot be created with group transaction events set to true when dispatcher threads is greater than 1");
  }

  @Test
  public void validateDoesNotThrowIfMustGroupTransactionEvents() {
    when(attributes.mustGroupTransactionEvents()).thenReturn(true);
    when(attributes.getDispatcherThreads()).thenReturn(1);

    assertThatNoException().isThrownBy(() -> factory.validate(attributes));
  }

  @Test
  public void validateMutatesOrderPolicyIfNullAndDispatcherThreadsGreaterThan1() {
    when(attributes.getOrderPolicy()).thenReturn(null);
    when(attributes.getDispatcherThreads()).thenReturn(2);

    assertThatNoException().isThrownBy(() -> factory.validate(attributes));

    verify(attributes).setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
  }

  @Test
  public void validateDoesNotMutateOrderPolicyIfNullAndDispatcherThreadsIs1() {
    when(attributes.getOrderPolicy()).thenReturn(null);
    when(attributes.getDispatcherThreads()).thenReturn(1);

    assertThatNoException().isThrownBy(() -> factory.validate(attributes));

    verify(attributes, never()).setOrderPolicy(any());
  }

  @Test
  public void validateDoesNotMutateOrderPolicyIfSet() {
    when(attributes.getOrderPolicy()).thenReturn(GatewaySender.OrderPolicy.KEY);
    when(attributes.getDispatcherThreads()).thenReturn(2);

    assertThatNoException().isThrownBy(() -> factory.validate(attributes));

    verify(attributes, never()).setOrderPolicy(any());
  }

}
