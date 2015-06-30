/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests PoolManager
 * @author darrel
 * @since 5.7
 */
@Category(IntegrationTest.class)
public class PoolManagerJUnitTest {
  
  private DistributedSystem ds;
  
  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    ds = DistributedSystem.connect(props);
    assertEquals(0, PoolManager.getAll().size());
  }
  
  @After
  public void tearDown() {
    PoolManager.close();
    ds.disconnect();
  }
  
  @Test
  public void testCreateFactory() {
    assertNotNull(PoolManager.createFactory());
    assertEquals(0, PoolManager.getAll().size());
  }

  @Test
  public void testGetMap() {
    assertEquals(0, PoolManager.getAll().size());
    {
      PoolFactory cpf = PoolManager.createFactory();
      ((PoolFactoryImpl)cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }
    assertEquals(1, PoolManager.getAll().size());
    {
      PoolFactory cpf = PoolManager.createFactory();
      ((PoolFactoryImpl)cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool2");
    }
    assertEquals(2, PoolManager.getAll().size());
    assertNotNull(PoolManager.getAll().get("mypool"));
    assertNotNull(PoolManager.getAll().get("mypool2"));
    assertEquals("mypool", (PoolManager.getAll().get("mypool")).getName());
    assertEquals("mypool2", (PoolManager.getAll().get("mypool2")).getName());
  }

  @Test
  public void testFind() {
    {
      PoolFactory cpf = PoolManager.createFactory();
      ((PoolFactoryImpl)cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }
    assertNotNull(PoolManager.find("mypool"));
    assertEquals("mypool", (PoolManager.find("mypool")).getName());
    assertEquals(null, PoolManager.find("bogus"));
  }

  @Test
  public void testRegionFind() {
    PoolFactory cpf = PoolManager.createFactory();
    ((PoolFactoryImpl)cpf).setStartDisabled(true);
    Pool pool = cpf.addLocator("localhost", 12345).create("mypool");
    Cache cache = CacheFactory.create(ds);
    AttributesFactory fact = new AttributesFactory();
    fact.setPoolName(pool.getName());
    Region region = cache.createRegion("myRegion", fact.create());
    assertEquals(pool, PoolManager.find(region));
  }

  @Test
  public void testClose() {
    PoolManager.close();
    assertEquals(0, PoolManager.getAll().size());
    {
      PoolFactory cpf = PoolManager.createFactory();
      ((PoolFactoryImpl)cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }
    assertEquals(1, PoolManager.getAll().size());
    PoolManager.close();
    assertEquals(0, PoolManager.getAll().size());
    {
      PoolFactory cpf = PoolManager.createFactory();
      ((PoolFactoryImpl)cpf).setStartDisabled(true);
      cpf.addLocator("localhost", 12345).create("mypool");
    }
    assertEquals(1, PoolManager.getAll().size());
    PoolManager.find("mypool").destroy();
    assertEquals(null, PoolManager.find("mypool"));
    assertEquals(0, PoolManager.getAll().size());
    PoolManager.close();
    assertEquals(0, PoolManager.getAll().size());
  }
}
