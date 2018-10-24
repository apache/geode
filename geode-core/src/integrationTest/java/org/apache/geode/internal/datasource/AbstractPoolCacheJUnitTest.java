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
 * AbstractPoolCacheJUnitTest.java JUnit based test
 *
 * Created on March 3, 2005, 5:24 PM
 */
package org.apache.geode.internal.datasource;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.util.test.TestUtil;

public class AbstractPoolCacheJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() throws Exception {
    props = new Properties();
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    String path = TestUtil.getResourcePath(AbstractPoolCacheJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty(CACHE_XML_FILE, path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
  }

  @After
  public void tearDown() throws Exception {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn = ds.getConnection();
    if (conn == null)
      fail(
          "DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
  }

  @Test
  public void testCleanUp() throws Exception {
    cache.close();
    ds1.disconnect();
  }

  /**
   * Tests if an XAresource obtained from an XAConnection which is already closed , can return null
   * or not.
   */
  @Ignore("TODO: test used to eat its own exception and it fails")
  @Test
  public void testEffectOfBlockingTimeoutOnXAConnection() throws Exception {
    Map map = new HashMap();
    map.put("init-pool-size", "2");
    map.put("jndi-name", "TestXAPooledDataSource");
    map.put("max-pool-size", "7");
    map.put("idle-timeout-seconds", "20");
    map.put("blocking-timeout-seconds", "2");
    map.put("login-timeout-seconds", "5");
    // map.put("xa-datasource-class","org.apache.derby.jdbc.EmbeddedXADataSource");
    map.put("jdbc-driver-class", "org.apache.derby.jdbc.EmbeddedDriver");
    map.put("user-name", "mitul");
    map.put("password", "83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a");
    map.put("connection-url", "jdbc:derby:newDB;create=true");
    List props = new ArrayList();
    props.add(new ConfigProperty("databaseName", "newDB", "java.lang.String"));

    GemFireBasicDataSource gbds =
        (GemFireBasicDataSource) new DataSourceFactory().getSimpleDataSource(map);
    map.put("xa-datasource-class", "org.apache.derby.jdbc.EmbeddedXADataSource");

    map.put("connection-url", "jdbc:derby:newDB;create=true");

    GemFireTransactionDataSource gtds =
        (GemFireTransactionDataSource) new DataSourceFactory().getTranxDataSource(map, props);

    XAConnection xaconn = (XAConnection) gtds.provider.borrowConnection();
    try {
      Thread.sleep(4);
    } catch (InterruptedException e) {
      fail("interrupted");
    }
    for (int i = 0; i < 1000; ++i) {
      XAResource xar = xaconn.getXAResource();
      System.out.println("XAResource=" + xar);
      assertNotNull(xar);
    }
  }
}
