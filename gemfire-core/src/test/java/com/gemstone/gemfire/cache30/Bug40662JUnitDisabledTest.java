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
