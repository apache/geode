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
package org.apache.geode.internal.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class SecurableCommunicationChannelTest {

  @Test
  public void getEnumShouldThrowExceptionWhenTheConstantIsNotKnown() {
    assertThatThrownBy(() -> SecurableCommunicationChannel.getEnum("none"))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessage("There is no registered component for the name: none");
  }

  @Test
  public void getEnumShouldReturnTheCorrectEnumWhenTheConstantIsKnown() {
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.ALL))
        .isEqualTo(SecurableCommunicationChannel.ALL);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.JMX))
        .isEqualTo(SecurableCommunicationChannel.JMX);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.WEB))
        .isEqualTo(SecurableCommunicationChannel.WEB);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.SERVER))
        .isEqualTo(SecurableCommunicationChannel.SERVER);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.CLUSTER))
        .isEqualTo(SecurableCommunicationChannel.CLUSTER);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.GATEWAY))
        .isEqualTo(SecurableCommunicationChannel.GATEWAY);
    assertThat(SecurableCommunicationChannel.getEnum(SecurableCommunicationChannels.LOCATOR))
        .isEqualTo(SecurableCommunicationChannel.LOCATOR);
  }
}
