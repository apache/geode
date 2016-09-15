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

import static org.apache.geode.test.dunit.Wait.pause;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public abstract class FunctionServiceClientAccessorPRBase extends FunctionServiceClientBase {

  public static final String REGION = "region";
  protected transient Region<Object, Object> region;

  @Before
  public void createRegions() {
    ClientCache cache = createServersAndClient(numberOfExecutions());

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    for(int i =0; i < numberOfExecutions(); i++) {
      VM vm = host.getVM(i);
      createRegion(vm);
    }

    vm0.invoke(() -> {
      Region region = getCache().getRegion(REGION);
      PartitionRegionHelper.assignBucketsToPartitions(region);
    });

    region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
      .create(REGION);
  }

  private void createRegion(final VM vm) {
    vm.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.PARTITION)
        .create(REGION);
      });
  }

  @Override
  public Execution getExecution() {
    return FunctionService.onRegion(region);
  }



  /**
   * Test that a custom result collector will still receive all partial
   * results from other members when one source fails
   */
  @Test
  public void nonHAFunctionResultCollectorIsPassedPartialResultsAfterBucketMove() {
    List<InternalDistributedMember> members = getAllMembers();
    //Only run this test if there is more than two members
    Assume.assumeTrue(members.size() >= 2);

    final Iterator<InternalDistributedMember> iterator = members.iterator();
    InternalDistributedMember firstMember = iterator.next();
    InternalDistributedMember secondMember = iterator.next();

    //Execute a function which will close the cache on one source.
    try {
      ResultCollector rc = getExecution()
        .withCollector(customCollector)
        .execute(new BucketMovingNonHAFunction(firstMember, secondMember));
      rc.getResult();
      fail("Should have thrown an exception");
    } catch(Exception expected) {
      //do nothing
    }
    assertEquals(new HashSet(members), new HashSet(customCollector.getResult()));
    assertEquals(numberOfExecutions(), customCollector.getResult().size());
  }

  /**
   * A function which will close the cache if the given source matches
   * the source executing this function
   */
  private class BucketMovingNonHAFunction implements Function {

    private final InternalDistributedMember source;
    private final InternalDistributedMember destination;

    public BucketMovingNonHAFunction(final InternalDistributedMember source, final InternalDistributedMember destination) {
      this.source = source;
      this.destination = destination;
    }

    @Override
    public void execute(FunctionContext context) {
      RegionFunctionContext regionFunctionContext = (RegionFunctionContext) context;
      final InternalDistributedMember myId = InternalDistributedSystem.getAnyInstance().getDistributedMember();
      //Move all buckets to the destination
      if (myId.equals(source)) {
        PartitionRegionHelper.moveData(regionFunctionContext.getDataSet(), source, destination, 100);
      }
      pause(1000);
      context.getResultSender().lastResult(myId);
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }
}
