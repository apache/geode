/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Created on Apr 19, 2005
 *
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;

@Category(IntegrationTest.class)
public class DeclarativeIndexCreationJUnitTest {

  private DistributedSystem ds;
  private Cache cache = null;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE, TestUtil.getResourcePath(getClass(), "cachequeryindex.xml"));
    props.setProperty(MCAST_PORT, "0");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    } finally {
      ds.disconnect();
    }
  }

  @Test
  public void testAsynchronousIndexCreatedOnRoot_PortfoliosRegion() {
    Region root = cache.getRegion("/root/portfolios");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      fail("DeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  @Test
  public void testSynchronousIndexCreatedOnRoot_StringRegion() {
    Region root = cache.getRegion("/root/string");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      fail("DeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
      fail("DeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }

  @Test
  public void testSynchronousIndexCreatedOnRootRegion() {
    Region root = cache.getRegion("/root");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      fail("DeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }
}
