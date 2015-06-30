/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Declarable;

/**
 * @author gthombar
 * This class is cacheLoader for the partition region
 */
public class PartitionedRegionCacheLoaderForSubRegion implements CacheLoader, Declarable {

	public Object load(LoaderHelper helper) throws CacheLoaderException {

		/* checking the attributes set in xml file */
		PartitionedRegion pr = (PartitionedRegion) helper.getRegion();
		if (pr.getAttributes().getPartitionAttributes().getRedundantCopies() != 1)
			Assert
					.fail("Redundancy of the partition region is not 1");
		
		Assert.assertEquals(pr.getAttributes()
				.getPartitionAttributes().getGlobalProperties().getProperty(
						PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY),
				"11");
		Assert.assertEquals(pr.getAttributes()
				.getPartitionAttributes().getLocalMaxMemory(), 200);
		/*
		 * Returning the same key. This is to check CaccheLoader is invoked or
		 * not
		 */
		return helper.getKey();

	}

	public void close() {

	}

	public void init(Properties props) {

	}

}
