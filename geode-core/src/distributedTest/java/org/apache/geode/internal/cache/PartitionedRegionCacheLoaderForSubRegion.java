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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;

/**
 * This class is cacheLoader for the partition region
 */
public class PartitionedRegionCacheLoaderForSubRegion implements CacheLoader, Declarable {

  @Override
  public Object load(LoaderHelper helper) throws CacheLoaderException {

    /* checking the attributes set in xml file */
    PartitionedRegion pr = (PartitionedRegion) helper.getRegion();
    if (pr.getAttributes().getPartitionAttributes().getRedundantCopies() != 1)
      fail("Redundancy of the partition region is not 1");

    assertEquals(pr.getAttributes().getPartitionAttributes().getGlobalProperties()
        .getProperty(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY), "11");
    assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 200);
    /*
     * Returning the same key. This is to check CaccheLoader is invoked or not
     */
    return helper.getKey();
  }

  @Override
  public void close() {}

  @Override
  public void init(Properties props) {}

}
