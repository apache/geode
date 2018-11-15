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

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.util.BlobHelper;

public class PartitionRegionConfigTest {

  private int prId;
  private String path;
  private PartitionAttributes partitionAttributes;
  private Scope scope;
  private EvictionAttributes evictionAttributes;
  private ExpirationAttributes regionIdleTimeout;
  private ExpirationAttributes regionTimeToLive;
  private ExpirationAttributes entryIdleTimeout;
  private ExpirationAttributes entryTimeToLive;
  private Set<String> gatewaySenderIds;

  @Before
  public void setUp() {
    prId = 0;
    path = null;
    partitionAttributes = new PartitionAttributesFactory().create();
    scope = Scope.DISTRIBUTED_ACK;
    evictionAttributes = new EvictionAttributesImpl();
    regionIdleTimeout = new ExpirationAttributes();
    regionTimeToLive = new ExpirationAttributes();
    entryIdleTimeout = new ExpirationAttributes();
    entryTimeToLive = new ExpirationAttributes();
    gatewaySenderIds = Collections.emptySet();
  }

  @Test
  public void dataSerializes() throws Exception {
    PartitionRegionConfig config = new PartitionRegionConfig(prId, path, partitionAttributes, scope,
        evictionAttributes, regionIdleTimeout, regionTimeToLive, entryIdleTimeout, entryTimeToLive,
        gatewaySenderIds);
    byte[] bytes = BlobHelper.serializeToBlob(config);
    assertThat(bytes).isNotNull().isNotEmpty();
    assertThat(BlobHelper.deserializeBlob(bytes)).isNotSameAs(config)
        .isInstanceOf(PartitionRegionConfig.class);
  }

  @Ignore("GEODE-4812")
  @Test
  public void serializes() throws Exception {
    PartitionRegionConfig config = new PartitionRegionConfig();
    byte[] bytes = SerializationUtils.serialize(config);
    assertThat(bytes).isNotNull().isNotEmpty();
    assertThat((PartitionRegionConfig) SerializationUtils.deserialize(bytes)).isNotSameAs(config)
        .isInstanceOf(PartitionRegionConfig.class);
  }
}
