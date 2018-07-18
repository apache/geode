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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;

public class EvictionAttributesMutatorImplRegressionTest {
  private Region<String, String> region;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    region = createRegion();
  }

  private Region<String, String> createRegion() throws Exception {
    int initMaximum = 1;
    Cache cache = new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    Region<String, String> aRegion =
        cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
            .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(initMaximum))
            .create(testName.getMethodName());

    RegionAttributes<String, String> attributes = aRegion.getAttributes();
    EvictionAttributes evictionAttributes = attributes.getEvictionAttributes();
    assertThat(evictionAttributes.getMaximum()).isEqualTo(initMaximum);
    return aRegion;
  }

  @Test
  public void verifySetMaximum() {
    AttributesMutator<String, String> attributesMutator = region.getAttributesMutator();
    EvictionAttributesMutator evictionAttributesMutator =
        attributesMutator.getEvictionAttributesMutator();
    int updatedMaximum = 2;
    evictionAttributesMutator.setMaximum(updatedMaximum);
    RegionAttributes<String, String> attributes = region.getAttributes();
    EvictionAttributes evictionAttributes = attributes.getEvictionAttributes();
    assertThat(evictionAttributes.getMaximum()).isEqualTo(updatedMaximum);
  }

}
