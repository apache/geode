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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.util.test.TestUtil;

/**
 * This class tests regions created by xml files
 */
@Category(DistributedTest.class)
public class PartitionedRegionCacheXMLExampleDUnitTest extends PartitionedRegionDUnitTestCase {

  protected static Cache cache;

  @Test
  public void testExampleWithBothRootRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    CacheSerializableRunnable createCache = new CacheSerializableRunnable("createCache") {
      public void run2() throws CacheException {

        Properties props = new Properties();
        String xmlfilepath =
            TestUtil.getResourcePath(getClass(), "PartitionRegionCacheExample1.xml");
        props.setProperty(CACHE_XML_FILE, xmlfilepath);

        getSystem(props);
        cache = getCache();
      }
    };

    CacheSerializableRunnable validateRegion = new CacheSerializableRunnable("validateRegion") {
      public void run2() throws CacheException {
        PartitionedRegion pr1 =
            (PartitionedRegion) cache.getRegion(Region.SEPARATOR + "firstPartitionRegion");
        assertNotNull("Partiton region cannot be null", pr1);
        Object obj = pr1.get("1");
        assertNotNull("CacheLoader is not invoked", obj);
        pr1.put("key1", "value1");
        assertEquals(pr1.get("key1"), "value1");

        PartitionedRegion pr2 =
            (PartitionedRegion) cache.getRegion(Region.SEPARATOR + "secondPartitionedRegion");
        assertNotNull("Partiton region cannot be null", pr2);
        pr2.put("key2", "value2");
        assertEquals(pr2.get("key2"), "value2");

      }
    };
    CacheSerializableRunnable disconnect = new CacheSerializableRunnable("disconnect") {
      public void run2() throws CacheException {
        closeCache();
        disconnectFromDS();
      }
    };

    vm0.invoke(createCache);
    vm1.invoke(createCache);
    vm0.invoke(validateRegion);
    // Disconnecting and again creating the PR from xml file
    vm1.invoke(disconnect);
    vm1.invoke(createCache);
    vm1.invoke(validateRegion);
  }

  @Test
  public void testExampleWithSubRegion() {
    Host host = Host.getHost(0);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    CacheSerializableRunnable createCacheSubregion =
        new CacheSerializableRunnable("createCacheSubregion") {

          public void run2() throws CacheException {

            Properties props = new Properties();
            String xmlfilepath =
                TestUtil.getResourcePath(getClass(), "PartitionRegionCacheExample2.xml");
            props.setProperty(CACHE_XML_FILE, xmlfilepath);
            getSystem(props);
            cache = getCache();
          }
        };

    CacheSerializableRunnable validateSubRegion = new CacheSerializableRunnable("validateRegion") {
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) cache
            .getRegion(Region.SEPARATOR + "root" + Region.SEPARATOR + "PartitionedSubRegion");
        assertNotNull("Partiton region cannot be null", pr);
        assertTrue(PartitionedRegionHelper.isSubRegion(pr.getFullPath()));
        Object obj = pr.get("1");
        assertNotNull("CacheLoader is not invoked", obj);
        pr.put("key1", "value1");
        assertEquals(pr.get("key1"), "value1");

      }
    };

    vm2.invoke(createCacheSubregion);
    vm3.invoke(createCacheSubregion);
    vm2.invoke(validateSubRegion);
  }
}
