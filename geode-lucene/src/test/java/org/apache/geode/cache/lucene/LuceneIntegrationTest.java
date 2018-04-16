/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import org.junit.After;
import org.junit.Before;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;


public class LuceneIntegrationTest {

  protected Cache cache;
  protected LuceneService luceneService;

  @After
  public void closeCache() {
    if (this.cache != null) {
      this.cache.close();
    }
  }

  @Before
  public void createCache() {
    CacheFactory cf = getCacheFactory();
    this.cache = cf.create();

    luceneService = LuceneServiceProvider.get(this.cache);
  }

  protected CacheFactory getCacheFactory() {
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.set(ConfigurationProperties.LOCATORS, "");
    cf.set(ConfigurationProperties.LOG_LEVEL, System.getProperty("logLevel", "info"));
    return cf;
  }

  protected Region createRegion(String regionName, RegionShortcut shortcut) {
    return this.cache.createRegionFactory(shortcut).create(regionName);
  }
}
