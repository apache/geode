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
package org.apache.geode.metrics;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.internal.InternalClientCache;

public class ClientCacheCommonTagsTest {

  private InternalClientCache clientCache;

  @After
  public void tearDown() {
    if (clientCache != null) {
      clientCache.close();
    }
  }

  @Test
  public void metersDoNotHaveClusterTag() {
    clientCache =
        (InternalClientCache) new ClientCacheFactory().set(DISTRIBUTED_SYSTEM_ID, "1").create();
    MeterRegistry meterRegistry = clientCache.getMeterRegistry();
    List<Meter> meters = meterRegistry.getMeters();

    assertThat(meters)
        .isNotEmpty();

    for (Meter meter : meters) {
      Meter.Id meterId = meter.getId();
      List<String> tagNames = meterId.getTags().stream().map(Tag::getKey).collect(toList());

      assertThat(tagNames)
          .as("Tag names for meter with name " + meterId.getName())
          .doesNotContain("cluster");
    }
  }
}
