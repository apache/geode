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
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * Test for Bug no. 40662. To verify the default action being set in eviction
 * attributes by CacheXmlParser when cache.xml has eviction attributes with no
 * eviction action specified. which was being set to EvictionAction.NONE
 * 
 * @author shoagarwal
 * @since 6.6
 */
public class Bug40662JUnitDisabledTest extends TestCase {

  private static final String BUG_40662_XML = Bug40662JUnitDisabledTest.class.getResource("bug40662noevictionaction.xml").getFile();

  DistributedSystem ds;
  Cache cache;

  @Override
  public void setName(String name) {
    super.setName(name);
  }

  /**
   * Test for checking eviction action in eviction attributes if no evicition
   * action is specified in cache.xml
   */
  public void testEvictionActionSetLocalDestroyPass() {
    Region exampleRegion = this.cache.getRegion("example-region");
    RegionAttributes<Object, Object> attrs = exampleRegion.getAttributes();
    EvictionAttributes evicAttrs = attrs.getEvictionAttributes();

    //Default eviction action is LOCAL_DESTROY always. 
    assertEquals(EvictionAction.LOCAL_DESTROY, evicAttrs.getAction());
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, BUG_40662_XML);
    this.ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(this.ds);
  }

}
