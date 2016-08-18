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
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

/**
 * Tests function execution with a client accessing multiple members with a PR with redundancy 0
 * using onRegion calls.
 */
@Category(DistributedTest.class)
public class FunctionServiceClientAccessorPRMultipleMembersDUnitTest extends FunctionServiceClientAccessorPRBase {

  @Override public void createRegions() {
    super.createRegions();
    //Make sure the client is consistently using singlehop by proactively getting
    //the server location metadata.
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) region);
  }

  @Override
  public int numberOfExecutions() {
    return 2;
  }
}
