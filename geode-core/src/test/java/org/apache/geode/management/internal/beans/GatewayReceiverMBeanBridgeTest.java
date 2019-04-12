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
package org.apache.geode.management.internal.beans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class GatewayReceiverMBeanBridgeTest {

  @Test
  public void getStartPortDelegatesToGatewayReceiver() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    int startPort = 42;
    when(gatewayReceiver.getStartPort()).thenReturn(startPort);
    GatewayReceiverMBeanBridge bridge = new GatewayReceiverMBeanBridge(gatewayReceiver);

    int value = bridge.getStartPort();

    assertThat(value).isEqualTo(startPort);
  }

  @Test
  public void getEndPortDelegatesToGatewayReceiver() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    int endPort = 84;
    when(gatewayReceiver.getEndPort()).thenReturn(endPort);
    GatewayReceiverMBeanBridge bridge = new GatewayReceiverMBeanBridge(gatewayReceiver);

    int value = bridge.getEndPort();

    assertThat(value).isEqualTo(endPort);
  }
}
