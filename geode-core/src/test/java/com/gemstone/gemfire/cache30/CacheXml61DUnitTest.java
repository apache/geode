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
package com.gemstone.gemfire.cache30;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Tests 6.1 cache.xml features.
 * 
 * @since GemFire 6.1
 */
@Category(DistributedTest.class)
public class CacheXml61DUnitTest extends CacheXml60DUnitTest {
  
  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_6_1;
  }


  /**
   * Tests that a region created with a named attributes set programmatically
   * for delta propogation has the correct attributes.
   * 
   */
  @Test
  public void testRegionAttributesForRegionEntryCloning() throws CacheException
  {
    final String rNameBase = getUniqueName();
    final String r1 = rNameBase + "1";

    // Setting multi-cast via nested region attributes
    CacheCreation creation = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(false);
    attrs.setCloningEnable(false);
    attrs.setMulticastEnabled(true);
    creation.createRegion(r1, attrs);
    
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    Region reg1 = c.getRegion(r1);
    assertNotNull(reg1);
    assertEquals(Scope.LOCAL, reg1.getAttributes().getScope());
    assertFalse(reg1.getAttributes().getEarlyAck());
    assertTrue(reg1.getAttributes().getMulticastEnabled());
    assertFalse(reg1.getAttributes().getCloningEnabled());
    
    //  changing Clonned setting
    reg1.getAttributesMutator().setCloningEnabled(true);
    assertTrue(reg1.getAttributes().getCloningEnabled());

    reg1.getAttributesMutator().setCloningEnabled(false);
    assertFalse(reg1.getAttributes().getCloningEnabled());
    
    // for sub region - a child attribute should be inherited
    String sub = "subRegion";
    RegionAttributesCreation attrsSub = new RegionAttributesCreation(creation);
    attrsSub.setScope(Scope.LOCAL);
    reg1.createSubregion(sub, attrsSub);
    Region subRegion = reg1.getSubregion(sub);
    assertFalse(subRegion.getAttributes().getCloningEnabled());
    subRegion.getAttributesMutator().setCloningEnabled(true);
    assertTrue(subRegion.getAttributes().getCloningEnabled());
  }
}
