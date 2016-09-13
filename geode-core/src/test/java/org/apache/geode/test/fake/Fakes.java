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
package org.apache.geode.test.fake;

import org.apache.geode.CancelCriterion;
import org.apache.geode.LogWriter;
import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.File;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Factory methods for fake objects for use in test.
 * 
 * These fakes are essentially mock objects with some limited
 * functionality. For example the fake cache can return a fake
 * distributed system.
 * 
 * All of the fakes returned by this class are Mockito.mocks, so
 * they can be modified by using Mockito stubbing, ie
 * 
 * <pre>
 * cache = Fakes.cache();
 * Mockito.when(cache.getName()).thenReturn(...)
 * <pre>
 * 
 * Please help extend this class by adding other commonly
 * used objects to this collection of fakes.
 */
public class Fakes {
  
  /**
   * A fake cache, which contains a fake distributed
   * system, distribution manager, etc.
   */
  public static GemFireCacheImpl cache() {
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    DistributionConfig config = mock(DistributionConfig.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    CancelCriterion systemCancelCriterion = mock(CancelCriterion.class);
    DSClock clock = mock(DSClock.class);
    LogWriter logger = mock(LogWriter.class);
    Statistics stats = mock(Statistics.class);
    
    InternalDistributedMember member;
    try {
      member = new InternalDistributedMember("localhost", 5555);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    
    when(config.getCacheXmlFile()).thenReturn(new File(""));
    when(config.getDeployWorkingDir()).thenReturn(new File("."));
    
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getMyId()).thenReturn(member);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    when(cache.getCancelCriterion()).thenReturn(systemCancelCriterion);
    
    when(system.getDistributedMember()).thenReturn(member);
    when(system.getConfig()).thenReturn(config);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    when(system.getCancelCriterion()).thenReturn(systemCancelCriterion);
    when(system.getClock()).thenReturn(clock);
    when(system.getLogWriter()).thenReturn(logger);
    when(system.createAtomicStatistics(any(), any(), anyLong())).thenReturn(stats);
    when(system.createAtomicStatistics(any(), any())).thenReturn(stats);

    when(distributionManager.getId()).thenReturn(member);
    when(distributionManager.getConfig()).thenReturn(config);
    when(distributionManager.getSystem()).thenReturn(system);
    when(distributionManager.getCancelCriterion()).thenReturn(systemCancelCriterion);
    
    return cache;
  }

  /**
   * A fake distributed system, which contains a fake distribution manager.
   */
  public static InternalDistributedSystem distributedSystem() {
    return cache().getDistributedSystem();
  }

  /**
   * A fake region, which contains a fake cache and some other
   * fake attributes
   */
  public static Region region(String name, Cache cache) {
    Region region = mock(Region.class);
    RegionAttributes attributes = mock(RegionAttributes.class);
    DataPolicy policy = mock(DataPolicy.class);
    when(region.getAttributes()).thenReturn(attributes);
    when(attributes.getDataPolicy()).thenReturn(policy);
    when(region.getCache()).thenReturn(cache);
    when(region.getRegionService()).thenReturn(cache);
    return region;
  }

  /**
   * Add real map behavior to a mock region. Useful for tests
   * where you want to mock region that just behaves like a map.
   * @param mock the mockito mock to add behavior too.
   */
  public static void addMapBehavior(Region mock) {
    //Allow the region to behave like a fake map
    Map underlyingMap = new HashMap();
    when(mock.get(any()))
      .then(invocation -> underlyingMap.get(invocation.getArguments()[0]));
    when(mock.put(any(), any()))
      .then(invocation -> underlyingMap.put(invocation.getArguments()[0], invocation.getArguments()[1]));
  }

  private Fakes() {
  }

}
