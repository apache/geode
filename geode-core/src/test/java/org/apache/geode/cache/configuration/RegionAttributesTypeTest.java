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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.configuration.RegionAttributesType.EvictionAttributes;
import org.apache.geode.cache.configuration.RegionAttributesType.ExpirationAttributesType;

public class RegionAttributesTypeTest {

  private ExpirationAttributesType expirationAttributes;
  private RegionAttributesType regionAttributes;

  @Before
  public void before() throws Exception {
    regionAttributes = new RegionAttributesType();
    expirationAttributes = new ExpirationAttributesType();
  }

  @Test
  public void emptyConstructor() {
    expirationAttributes = new ExpirationAttributesType();
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void expirationAttributesConstructor() {
    expirationAttributes =
        new ExpirationAttributesType(null, ExpirationAction.DESTROY.toXmlString(), null,
            null);
    assertThat(expirationAttributes.getAction()).isEqualTo("destroy");
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isTrue();
    assertThat(expirationAttributes.hasCustomExpiry()).isFalse();

    expirationAttributes = new ExpirationAttributesType(10, null, null, null);
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isEqualTo("10");
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isTrue();
    assertThat(expirationAttributes.hasCustomExpiry()).isFalse();

    expirationAttributes =
        new ExpirationAttributesType(null, null, "abc", null);
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getTimeout()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNotNull();
    assertThat(expirationAttributes.hasTimoutOrAction()).isFalse();
    assertThat(expirationAttributes.hasCustomExpiry()).isTrue();
  }

  @Test
  public void generateExpirationAttributes() {
    expirationAttributes = ExpirationAttributesType.generate(null, null, null);
    assertThat(expirationAttributes).isNull();

    expirationAttributes = ExpirationAttributesType.generate(8, null, null);
    assertThat(expirationAttributes.getTimeout()).isEqualTo("8");
    assertThat(expirationAttributes.getAction()).isNull();
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
  }

  @Test
  public void combineExpirationAttributes() throws Exception {
    expirationAttributes = new ExpirationAttributesType(8, null, null, null);
    expirationAttributes = ExpirationAttributesType.combine(null, expirationAttributes);
    assertThat(expirationAttributes.getTimeout()).isEqualTo("8");
    assertThat(expirationAttributes.getAction())
        .isEqualTo(ExpirationAction.INVALIDATE.toXmlString());
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(ExpirationAttributesType.combine(expirationAttributes, null))
        .isEqualToComparingFieldByFieldRecursively(expirationAttributes);

    ExpirationAttributesType another =
        new ExpirationAttributesType(null, ExpirationAction.DESTROY.toXmlString(), "abc", null);
    expirationAttributes = ExpirationAttributesType.combine(expirationAttributes, another);
    assertThat(expirationAttributes.getTimeout()).isEqualTo("8");
    assertThat(expirationAttributes.getAction()).isEqualTo(ExpirationAction.DESTROY.toXmlString());
    assertThat(expirationAttributes.getCustomExpiry().getClassName()).isEqualTo("abc");
  }

  @Test
  public void expirationAttributesDetail() throws Exception {
    assertThatThrownBy(() -> new ExpirationAttributesType(8, "invalid", null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void generateEvictionAttributes() {
    EvictionAttributes evictionAttributes = EvictionAttributes.generate(null, null, null, null);
    assertThat(evictionAttributes).isNull();

    assertThatThrownBy(() -> EvictionAttributes.generate(null, 8, null, null))
        .isInstanceOf(IllegalArgumentException.class);

    evictionAttributes = EvictionAttributes.generate("local-destroy", null, null, null);
    assertThat(evictionAttributes.getLruHeapPercentage().getAction().value())
        .isEqualTo("local-destroy");
    assertThat(evictionAttributes.getLruMemorySize()).isNull();
    assertThat(evictionAttributes.getLruEntryCount()).isNull();

    evictionAttributes = EvictionAttributes.generate("local-destroy", 10, null, null);
    assertThat(evictionAttributes.getLruHeapPercentage()).isNull();
    assertThat(evictionAttributes.getLruMemorySize().getMaximum()).isEqualTo("10");
    assertThat(evictionAttributes.getLruMemorySize().getAction().value())
        .isEqualTo("local-destroy");
    assertThat(evictionAttributes.getLruEntryCount()).isNull();

    // maxEntryCount is ignored when maxMemory is specified
    evictionAttributes = EvictionAttributes.generate("local-destroy", 10, 20, null);
    assertThat(evictionAttributes.getLruHeapPercentage()).isNull();
    assertThat(evictionAttributes.getLruMemorySize().getMaximum()).isEqualTo("10");
    assertThat(evictionAttributes.getLruMemorySize().getAction().value())
        .isEqualTo("local-destroy");
    assertThat(evictionAttributes.getLruEntryCount()).isNull();

    evictionAttributes = EvictionAttributes.generate("local-destroy", null, 20, null);
    assertThat(evictionAttributes.getLruHeapPercentage()).isNull();
    assertThat(evictionAttributes.getLruMemorySize()).isNull();
    assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("20");
    assertThat(evictionAttributes.getLruEntryCount().getAction().value())
        .isEqualTo("local-destroy");
  }

  @Test
  public void generatePartitionAttributes() throws Exception {
    assertThat(RegionAttributesType.PartitionAttributes.generate(null, null, null, null, null, null,
        null, null, null)).isNull();
  }

  @Test
  public void gatewaySender() {
    regionAttributes.setGatewaySenderIds(null);
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNull();

    regionAttributes.setGatewaySenderIds("");
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNotNull().isEmpty();

    regionAttributes.setGatewaySenderIds("abc,def");
    assertThat(regionAttributes.getGatewaySenderIdsAsSet()).isNotNull().hasSize(2);
  }

  @Test
  public void asyncEventQueue() {
    regionAttributes.setAsyncEventQueueIds(null);
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNull();

    regionAttributes.setAsyncEventQueueIds("");
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNotNull().isEmpty();

    regionAttributes.setAsyncEventQueueIds("abc,def");
    assertThat(regionAttributes.getAsyncEventQueueIdsAsSet()).isNotNull().hasSize(2);
  }
}
