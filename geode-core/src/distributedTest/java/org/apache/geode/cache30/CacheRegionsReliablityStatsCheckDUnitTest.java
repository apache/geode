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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.test.dunit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class CacheRegionsReliablityStatsCheckDUnitTest extends JUnit4CacheTestCase {

  /**
   * The tests check to see if all the reliablity stats are working fine and asserts their values to
   * constants.
   */
  @Test
  public void testRegionsReliablityStats() throws Exception {
    final String rr1 = "roleA";
    final String regionNoAccess = "regionNoAccess";
    final String regionLimitedAccess = "regionLimitedAccess";
    final String regionFullAccess = "regionFullAccess";
    // final String regionNameRoleA = "roleA";
    String[] requiredRoles = {rr1};
    Cache myCache = getCache();

    MembershipAttributes ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    fac.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attr = fac.create();
    myCache.createRegion(regionNoAccess, attr);

    ra = new MembershipAttributes(requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    fac.setDataPolicy(DataPolicy.REPLICATE);
    attr = fac.create();
    myCache.createRegion(regionLimitedAccess, attr);

    ra = new MembershipAttributes(requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    fac.setDataPolicy(DataPolicy.REPLICATE);
    attr = fac.create();
    myCache.createRegion(regionFullAccess, attr);

    CachePerfStats stats = ((GemFireCacheImpl) myCache).getCachePerfStats();

    assertEquals(stats.getReliableRegionsMissingNoAccess(), 1);
    assertEquals(stats.getReliableRegionsMissingLimitedAccess(), 1);
    assertEquals(stats.getReliableRegionsMissingFullAccess(), 1);
    assertEquals(stats.getReliableRegionsMissing(),
        (stats.getReliableRegionsMissingNoAccess() + stats.getReliableRegionsMissingLimitedAccess()
            + stats.getReliableRegionsMissingFullAccess()));

    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable roleAPlayer = new CacheSerializableRunnable("ROLEAPLAYER") {
      @Override
      public void run2() throws CacheException {

        Properties props = new Properties();
        props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        props.setProperty(ROLES, rr1);

        getSystem(props);
        Cache cache = getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        cache.createRegion(regionNoAccess, attr);
        cache.createRegion(regionLimitedAccess, attr);
        cache.createRegion(regionFullAccess, attr);

      }

    };

    vm1.invoke(roleAPlayer);

    assertEquals(stats.getReliableRegionsMissingNoAccess(), 0);
    assertEquals(stats.getReliableRegionsMissingLimitedAccess(), 0);
    assertEquals(stats.getReliableRegionsMissingFullAccess(), 0);
    assertEquals(stats.getReliableRegionsMissing(),
        (stats.getReliableRegionsMissingNoAccess() + stats.getReliableRegionsMissingLimitedAccess()
            + stats.getReliableRegionsMissingFullAccess()));

  }

}
