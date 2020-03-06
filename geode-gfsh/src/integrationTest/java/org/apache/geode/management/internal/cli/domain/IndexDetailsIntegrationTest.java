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

package org.apache.geode.management.internal.cli.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class IndexDetailsIntegrationTest {

  private static final String REGION_1 = "REGION1";
  private static final String INDEX_REGION_NAME = "/REGION1";
  private static final String INDEX_1 = "INDEX1";

  @SuppressWarnings("deprecation")
  private static final org.apache.geode.cache.query.IndexType FUNCTIONAL =
      org.apache.geode.cache.query.IndexType.FUNCTIONAL;

  private Region<Integer, Stock> region;

  @Rule
  public ServerStarterRule serverRule =
      new ServerStarterRule().withRegion(RegionShortcut.REPLICATE, REGION_1);

  @Before
  public void before() throws Exception {
    Cache cache = serverRule.getCache();
    region = cache.getRegion(REGION_1);

    cache.getQueryService().createIndex(INDEX_1, "key", INDEX_REGION_NAME);
    region.put(1, new Stock("SUNW", 10));
    region.get(1);
  }

  @Test
  public void basicAttributes() {
    Cache cache = serverRule.getCache();
    Index idx = cache.getQueryService().getIndex(region, INDEX_1);
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    IndexDetails details = new IndexDetails(member, idx);
    assertThat(details).isNotNull();

    assertThat(details.getIndexName()).isEqualTo(INDEX_1);
    assertThat(details.getRegionPath()).isEqualTo(INDEX_REGION_NAME);
    assertThat(details.getMemberId()).isEqualTo(member.getId());
    assertThat(details.getMemberName()).isEqualTo(member.getName());
    assertThat(details.getFromClause()).isEqualTo(INDEX_REGION_NAME);
    assertThat(details.getIndexedExpression()).isEqualTo("key");
    assertThat(details.getIndexType()).isEqualTo(FUNCTIONAL);
    assertThat(details.getProjectionAttributes()).isEqualTo("*");
    assertThat(details.getIsValid()).isEqualTo(true);

    IndexDetails.IndexStatisticsDetails stats = details.getIndexStatisticsDetails();
    assertThat(stats.getNumberOfKeys()).isEqualTo(1);
    assertThat(stats.getNumberOfUpdates()).isEqualTo(1);
    assertThat(stats.getNumberOfValues()).isEqualTo(1);
  }
}
