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
package org.apache.geode.pdx.internal;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PeerTypeRegistrationIntegrationTest {
  private Cache cache;
  private PeerTypeRegistration registration;

  @Before
  public void setUp() {
    cache = new CacheFactory().set(MCAST_PORT, "0")
        .set("log-level", "WARN").setPdxReadSerialized(true).create();
    registration = new PeerTypeRegistration((InternalCache) cache);
    registration.initialize();
  }

  @After
  public void tearDown() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testDefineType() {

    PdxType firstType = new PdxType();
    firstType.setClassName("Mock.Test.Class.One");

    PdxType secondType = new PdxType();
    secondType.setClassName("Mock.Test.Class.Two");

    assertThat(registration.getLocalSize()).isEqualTo(0);
    assertThat(registration.getTypeToIdSize()).isEqualTo(0);

    int firstTypeId1 = registration.defineType(firstType);

    // Confirm the PdxType was added to the region and the local map
    assertThat(registration.getLocalSize()).isEqualTo(1);
    assertThat(registration.getTypeToIdSize()).isEqualTo(1);

    firstType.setTypeId(firstTypeId1 - 1);
    int firstTypeId2 = registration.defineType(firstType);

    // Defining an existing type with a different TypeId returns the existing TypeId
    assertThat(firstTypeId1).isEqualTo(firstTypeId2);
    assertThat(registration.getType(firstTypeId2)).isEqualTo(firstType);

    // Defining an existing type does not add a new type to the region or local map
    assertThat(registration.getLocalSize()).isEqualTo(1);
    assertThat(registration.getTypeToIdSize()).isEqualTo(1);

    secondType.setTypeId(firstTypeId1);

    int secondTypeId = registration.defineType(secondType);

    // Defining a new type with an existing TypeId does not overwrite the existing type
    assertThat(secondTypeId).isNotEqualTo(firstTypeId1);
    assertThat(registration.getLocalSize()).isEqualTo(2);
    assertThat(registration.getTypeToIdSize()).isEqualTo(2);
  }
}
