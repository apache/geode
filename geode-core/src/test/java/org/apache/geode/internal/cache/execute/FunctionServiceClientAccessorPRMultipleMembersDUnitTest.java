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
package org.apache.geode.internal.cache.execute;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

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
