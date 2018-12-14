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
package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.ExpirationAction;

public class RegionAttributesTypeTest {

  private RegionAttributesType.ExpirationAttributesType expirationAttributes;
  private RegionAttributesType regionAttributes;

  @Before
  public void before() throws Exception {
    regionAttributes = new RegionAttributesType();
    expirationAttributes = new RegionAttributesType.ExpirationAttributesType();
  }

  @Test
  public void emptyConstructor() {
    expirationAttributes = new RegionAttributesType.ExpirationAttributesType();
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void constructorWithParameter() {
    expirationAttributes =
        new RegionAttributesType.ExpirationAttributesType(null, ExpirationAction.DESTROY, null,
            null);
    assertThat(expirationAttributes.getAction()).isEqualTo("destroy");
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isTrue();
    assertThat(expirationAttributes.hasCustomExpiry()).isFalse();

    expirationAttributes = new RegionAttributesType.ExpirationAttributesType(10, null, null, null);
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isEqualTo("10");
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isTrue();
    assertThat(expirationAttributes.hasCustomExpiry()).isFalse();

    expirationAttributes =
        new RegionAttributesType.ExpirationAttributesType(null, null, "abc", null);
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNotNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isFalse();
    assertThat(expirationAttributes.hasCustomExpiry()).isTrue();
  }

  @Test
  public void gatewaySender() throws Exception {
    regionAttributes.setGatewaySenderIds(null);
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNull();

    regionAttributes.setGatewaySenderIds("");
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNotNull().isEmpty();

    regionAttributes.setGatewaySenderIds("abc,def");
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNotNull().hasSize(2);
  }

  @Test
  public void asyncEventQueue() throws Exception {
    regionAttributes.setAsyncEventQueueIds(null);
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNull();

    regionAttributes.setAsyncEventQueueIds("");
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNotNull().isEmpty();

    regionAttributes.setAsyncEventQueueIds("abc,def");
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNotNull().hasSize(2);
  }
}
