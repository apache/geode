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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Test for Bug no. 40662. To verify the default action being set in eviction attributes by
 * CacheXmlParser when cache.xml has eviction attributes with no eviction action specified. which
 * was being set to EvictionAction.NONE
 *
 * <p>
 * TRAC #40662: LRU behavior different when action isn't specified
 *
 * @since GemFire 6.6
 */
public class DefaultEvictionActionRegressionTest {

  private static final String BUG_40662_XML = DefaultEvictionActionRegressionTest.class
      .getResource("DefaultEvictionActionRegressionTest_cache.xml").getFile();

  private DistributedSystem ds;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(CACHE_XML_FILE, BUG_40662_XML);
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  /**
   * Test for checking eviction action in eviction attributes if no evicition action is specified in
   * cache.xml
   */
  @Test
  public void testEvictionActionSetLocalDestroyPass() {
    Region exampleRegion = cache.getRegion("example-region");
    RegionAttributes<Object, Object> attrs = exampleRegion.getAttributes();
    EvictionAttributes evicAttrs = attrs.getEvictionAttributes();

    // Default eviction action is LOCAL_DESTROY always.
    assertEquals(EvictionAction.LOCAL_DESTROY, evicAttrs.getAction());
  }
}
