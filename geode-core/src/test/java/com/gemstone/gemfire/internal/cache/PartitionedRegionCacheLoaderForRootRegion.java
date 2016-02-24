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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Declarable;

/**
 * This class is CacheLoader for partition region
 */
public class PartitionedRegionCacheLoaderForRootRegion implements CacheLoader,
		Declarable {

	public Object load(LoaderHelper helper) throws CacheLoaderException {

		/* checking the attributes set in xml file. */
		PartitionedRegion pr = (PartitionedRegion) helper.getRegion();
		if (pr.getAttributes().getPartitionAttributes().getRedundantCopies() != 1)
			Assert
					.fail("Redundancy of the partition region is not 1");

		Assert.assertEquals(
                    pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 200);

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
