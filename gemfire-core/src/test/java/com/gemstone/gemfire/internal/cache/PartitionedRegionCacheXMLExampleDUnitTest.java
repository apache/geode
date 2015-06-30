/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import dunit.Host;
import dunit.VM;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * @author gthombar This class tests regions created by xml files
 */
public class PartitionedRegionCacheXMLExampleDUnitTest extends
		PartitionedRegionDUnitTestCase {

	protected static Cache cache;

	public PartitionedRegionCacheXMLExampleDUnitTest(String name) {
		super(name);
	}

	public void testExampleWithBothRootRegion() {
		Host host = Host.getHost(0);
		VM vm0 = host.getVM(0);
		VM vm1 = host.getVM(1);

		CacheSerializableRunnable createCache = new CacheSerializableRunnable(
				"createCache") {
			public void run2() throws CacheException {

				Properties props = new Properties();
				String xmlfilepath = TestUtil.getResourcePath(getClass(), "PartitionRegionCacheExample1.xml");
				props.setProperty("cache-xml-file", xmlfilepath);
                                
				getSystem(props);
				cache = getCache();
			}
		};

		CacheSerializableRunnable validateRegion = new CacheSerializableRunnable(
				"validateRegion") {
			public void run2() throws CacheException {
				PartitionedRegion pr1 = (PartitionedRegion) cache
						.getRegion(Region.SEPARATOR + "firstPartitionRegion");
				assertNotNull("Partiton region cannot be null", pr1);
				Object obj = pr1.get("1");
				assertNotNull("CacheLoader is not invoked", obj);
				pr1.put("key1", "value1");
				assertEquals(pr1.get("key1"), "value1");

				PartitionedRegion pr2 = (PartitionedRegion) cache
						.getRegion(Region.SEPARATOR + "secondPartitionedRegion");
				assertNotNull("Partiton region cannot be null", pr2);
				pr2.put("key2", "value2");
				assertEquals(pr2.get("key2"), "value2");

			}
		};
		CacheSerializableRunnable disconnect = new CacheSerializableRunnable(
				"disconnect") {
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

	public void testExampleWithSubRegion() {
		Host host = Host.getHost(0);
		VM vm2 = host.getVM(2);
		VM vm3 = host.getVM(3);

		CacheSerializableRunnable createCacheSubregion = new CacheSerializableRunnable(
				"createCacheSubregion") {

			public void run2() throws CacheException {

				Properties props = new Properties();
				String xmlfilepath = TestUtil.getResourcePath(getClass(), "PartitionRegionCacheExample2.xml");
				props.setProperty("cache-xml-file", xmlfilepath);
				getSystem(props);
				cache = getCache();
			}
		};

		CacheSerializableRunnable validateSubRegion = new CacheSerializableRunnable(
				"validateRegion") {
			public void run2() throws CacheException {
				PartitionedRegion pr = (PartitionedRegion) cache
						.getRegion(Region.SEPARATOR + "root" + Region.SEPARATOR
								+ "PartitionedSubRegion");
				assertNotNull("Partiton region cannot be null", pr);
				assertTrue(PartitionedRegionHelper
						.isSubRegion(pr.getFullPath()));
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