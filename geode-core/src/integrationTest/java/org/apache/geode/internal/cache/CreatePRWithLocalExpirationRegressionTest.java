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

import static org.apache.geode.cache.ExpirationAction.LOCAL_DESTROY;
import static org.apache.geode.cache.ExpirationAction.LOCAL_INVALIDATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;

/**
 * PRWithLocalExpirationRegressionTest
 *
 * TRAC #40632: PR expiration with localDestroy fails with InternalGemFireError
 */
public class CreatePRWithLocalExpirationRegressionTest {

  private Cache cache;
  private AttributesFactory attributesFactory;

  @Before
  public void setUp() {
    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRecoveryDelay(-1);
    paf.setRedundantCopies(1);
    paf.setStartupRecoveryDelay(-1);

    attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(paf.create());
    attributesFactory.setStatisticsEnabled(true);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createPrWithEntryIdleTimeoutLocalDestroyThrows() throws Exception {
    attributesFactory.setEntryIdleTimeout(new ExpirationAttributes(1000, LOCAL_DESTROY));

    assertThatThrownBy(() -> cache.createRegion("region1", attributesFactory.create()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createPrWithEntryTimeToLiveLocalDestroyThrows() throws Exception {
    attributesFactory.setEntryTimeToLive(new ExpirationAttributes(1000, LOCAL_DESTROY));

    assertThatThrownBy(() -> cache.createRegion("region1", attributesFactory.create()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createPrWithEntryIdleTimeoutLocalInvalidateThrows() throws Exception {
    attributesFactory.setEntryIdleTimeout(new ExpirationAttributes(1000, LOCAL_INVALIDATE));

    assertThatThrownBy(() -> cache.createRegion("region1", attributesFactory.create()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createPrWithEntryTimeToLiveLocalInvalidateThrows() throws Exception {
    attributesFactory.setEntryTimeToLive(new ExpirationAttributes(1000, LOCAL_INVALIDATE));

    assertThatThrownBy(() -> cache.createRegion("region1", attributesFactory.create()))
        .isInstanceOf(IllegalStateException.class);
  }
}
