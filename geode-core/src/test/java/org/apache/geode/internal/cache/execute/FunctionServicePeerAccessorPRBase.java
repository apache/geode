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
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

import org.junit.Before;
import org.junit.Ignore;

public abstract class FunctionServicePeerAccessorPRBase extends FunctionServiceBase {

  public static final String REGION = "region";
  private transient Region<Object, Object> region;

  @Before
  public void createRegions() {
    region = getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY)
      .create(REGION);
    Host host = Host.getHost(0);
    for(int i =0; i < numberOfExecutions(); i++) {
      VM vm = host.getVM(i);
      createRegion(vm);
    }

    PartitionRegionHelper.assignBucketsToPartitions(region);
  }

  private void createRegion(final VM vm) {
    vm.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.PARTITION)
        .create(REGION);
      });
  }

  @Override public Execution getExecution() {
    return FunctionService.onRegion(region);
  }

  @Ignore("GEODE-1348 - With this topology, the exception is not wrapped in FunctionException")
  @Override public void defaultCollectorThrowsExceptionAfterFunctionThrowsIllegalState() {
  }

  @Ignore("GEODE-1348 - With this topology, the exception is not wrapped in FunctionException")
  @Override public void customCollectorDoesNotSeeExceptionFunctionThrowsIllegalState() {
  }
}
