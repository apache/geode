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
/*
 * Created on Mar 22, 2005
 */
package org.apache.geode.internal.datasource;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.Properties;

import javax.naming.Context;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.util.test.TestUtil;


/**
 *
 * To change the template for this generated type comment go to Window - Preferences - Java - Code
 * Generation - Code and Comments
 */
public class CleanUpJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() {
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    String path = TestUtil.getResourcePath(CleanUpJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty(CACHE_XML_FILE, path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
  }

  @After
  public void tearDown() {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
        fail(
            "DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    } catch (Exception e) {
      fail("Exception occurred in testGetSimpleDataSource due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testCleanUp() {
    cache.close();
    ds1.disconnect();
  }
}
