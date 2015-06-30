/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Apr 19, 2005
 *
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Properties;

import junit.framework.Assert;

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
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author asifs
 * 
 *         To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Generation - Code and Comments
 */
@Category(IntegrationTest.class)
public class DeclarativeIndexCreationJUnitTest {

  private DistributedSystem ds;
  private Cache cache = null;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("cache-xml-file", TestUtil.getResourcePath(getClass(), "cachequeryindex.xml"));
    props.setProperty("mcast-port", "0");
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
      Assert.assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  @Test
  public void testSynchronousIndexCreatedOnRoot_StringRegion() {
    Region root = cache.getRegion("/root/string");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
      Assert
          .fail("DeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }

  @Test
  public void testSynchronousIndexCreatedOnRootRegion() {
    Region root = cache.getRegion("/root");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      CacheUtils.log("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }
}
