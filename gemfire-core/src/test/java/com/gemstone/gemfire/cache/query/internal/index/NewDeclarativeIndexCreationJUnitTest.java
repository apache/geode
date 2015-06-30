/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author shobhit
 *
 * @since 6.6.1
 */
@Category(IntegrationTest.class)
public class NewDeclarativeIndexCreationJUnitTest {

  private Cache cache = null;

  @Before
  public void setUp() throws Exception {
    //Read the Cache.xml placed in test.lib folder
    Properties props = new Properties();
    props.setProperty("cache-xml-file", TestUtil.getResourcePath(getClass(), "cachequeryindex.xml"));
    props.setProperty("mcast-port", "0");
    DistributedSystem ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    if (!cache.isClosed()) cache.close();
  }

  @Test
  public void testAsynchronousIndexCreatedOnRoot_PortfoliosRegion() {
    Region root = cache.getRegion("/root/portfolios");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      //System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  @Test
  public void testSynchronousIndexCreatedOnRoot_StringRegion() {
    Region root = cache.getRegion("/root/string");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    ;
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      //System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
        Assert
            .fail("NewDeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }

  @Test
  public void testSynchronousIndexCreatedOnRootRegion() {
    Region root = cache.getRegion("/root");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      //System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }
  

  // Index creation tests for new DTD changes for Index tag for 6.6.1 with no function/primary-key tag
  @Test
  public void testAsynchronousIndexCreatedOnPortfoliosRegionWithNewDTD() {
    Region root = cache.getRegion("/root/portfolios2");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      //System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationJUnitTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  @Test
  public void testSynchronousIndexCreatedOnStringRegionWithNewDTD() {
    Region root = cache.getRegion("/root/string2");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    ;
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      //System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationJUnitTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
        Assert
            .fail("DeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }
  
  @Test
  public void testIndexCreationExceptionOnRegionWithNewDTD() throws IOException, URISyntaxException {
    if (cache != null && !cache.isClosed()) cache.close();
    Properties props = new Properties();
    props.setProperty("cache-xml-file", TestUtil.getResourcePath(getClass(), "cachequeryindexwitherror.xml"));
    props.setProperty("mcast-port", "0");
    DistributedSystem ds = DistributedSystem.connect(props);
    try {
      Cache cache = CacheFactory.create(ds);
    } catch (CacheXmlException e) {
      if (!e.getCause().getMessage().contains("CacheXmlParser::endIndex:Index creation attribute not correctly specified.")) {
        e.printStackTrace();
        Assert.fail("NewDeclarativeIndexCreationJUnitTest::setup: Index creation should have thrown exception for index on /root/portfolios3 region.");
      }
      return;
    }
  }
}
