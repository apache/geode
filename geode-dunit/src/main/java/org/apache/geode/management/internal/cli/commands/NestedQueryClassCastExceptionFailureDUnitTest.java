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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.TreeMap;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.version.VersionManager;

public class NestedQueryClassCastExceptionFailureDUnitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void classCastExceptionShouldNotBeThrownWhileExecutionNestedQueries() {
    Host host = Host.getHost(0);
    VM server = host.getVM(VersionManager.CURRENT_VERSION, 0);
    server.invoke(() -> {
      Properties properties = new Properties();
      properties.put(SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.management.internal.cli.commands.*");
      Cache cache = new CacheFactory(properties)
          .setPdxSerializer(new ReflectionBasedAutoSerializer(
              "org.apache.geode.management.internal.cli.commands.*"))
          .create();

      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("product");
      QueryService queryService = cache.getQueryService();
      queryService.createKeyIndex("productIDPKIndex", "productID", "/product");
      queryService.createIndex("productIdIndex", "productId", "/product");
      queryService
          .createIndex("productCodesGMIIndex", "productCodes['GMI']", "/product");
      TreeMap<String, String> tempMap = new TreeMap<>();
      tempMap.put("GMI", "GMI");
      region.put(1, new Product(1L, tempMap, "2L", "ACTIVE"));
      region.put(2, new Product(2L, tempMap, null, "ACTIVE"));
      region.put(3, new Product(3L, tempMap, "2L", "ACTIVE"));
      region.put(4, new Product(4L, tempMap, null, "ACTIVE"));
      SelectResults results =
          (SelectResults) cache.getQueryService()
              .newQuery(
                  "select  productId, productCodes['GMI'], contractSize from /product where contractSize = null and productCodes['GMI'] in (select  distinct b.productCodes['GMI'] from /product b where b.contractSize != null and b.status='ACTIVE') LIMIT 2000")
              .execute();
      assertEquals(2, results.size());
    });
  }
}
