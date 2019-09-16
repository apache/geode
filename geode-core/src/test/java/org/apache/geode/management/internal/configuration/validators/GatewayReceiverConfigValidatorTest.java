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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.internal.CacheElementOperation;

public class GatewayReceiverConfigValidatorTest {

  private GatewayReceiver receiver;
  private GatewayReceiverConfigValidator validator;

  @Before
  public void before() throws Exception {
    receiver = new GatewayReceiver();
    validator = new GatewayReceiverConfigValidator();
  }

  @Test
  public void startPort() throws Exception {
    receiver.setStartPort(6000);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, receiver))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start port 6000 must be less than the end port 5500");
  }

  @Test
  public void endPort() throws Exception {
    receiver.setEndPort(4000);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, receiver))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start port 5000 must be less than the end port 4000");
  }

  @Test
  public void startPortEndPort() throws Exception {
    receiver.setStartPort(2000);
    receiver.setEndPort(1900);
    assertThatThrownBy(() -> validator.validate(CacheElementOperation.CREATE, receiver))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start port 2000 must be less than the end port 1900");
  }
}
