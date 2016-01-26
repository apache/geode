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
package com.gemstone.gemfire.cache30;


import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class CacheRegionsReliablityStatsCheckDUnitTest extends CacheTestCase {
	public CacheRegionsReliablityStatsCheckDUnitTest(String name) {
	    super(name);
	}
	
	/**The tests check to see if all the reliablity stats are working
	 * fine and asserts their values to constants.
	 * 
	 * */
	public void testRegionsReliablityStats() throws Exception, RegionExistsException{
		final String rr1 = "roleA";
		final String regionNoAccess = "regionNoAccess";
		final String regionLimitedAccess = "regionLimitedAccess";
		final String regionFullAccess = "regionFullAccess";
		//final String regionNameRoleA = "roleA";
		String requiredRoles[] = { rr1};
		Cache myCache = getCache();
		
		MembershipAttributes ra = new MembershipAttributes(requiredRoles,
		        LossAction.NO_ACCESS, ResumptionAction.NONE);
		
		AttributesFactory fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    
	    RegionAttributes attr = fac.create();
	    myCache.createRegion(regionNoAccess, attr);
	    
	    ra = new MembershipAttributes(requiredRoles,
		        LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.create();
		myCache.createRegion(regionLimitedAccess, attr);
		
		ra = new MembershipAttributes(requiredRoles,
		        LossAction.FULL_ACCESS, ResumptionAction.NONE);
		fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.create();
		myCache.createRegion(regionFullAccess, attr);
		
		CachePerfStats stats = ((GemFireCacheImpl)myCache).getCachePerfStats();
		
		assertEquals(stats.getReliableRegionsMissingNoAccess(), 1);
		assertEquals(stats.getReliableRegionsMissingLimitedAccess(), 1);
		assertEquals(stats.getReliableRegionsMissingFullAccess(), 1);
	    assertEquals(stats.getReliableRegionsMissing(), (stats.getReliableRegionsMissingNoAccess() + 
	    		stats.getReliableRegionsMissingLimitedAccess() + stats.getReliableRegionsMissingFullAccess() ));
		
	    
	    Host host = Host.getHost(0);
	    VM vm1 = host.getVM(1);
	    
	    SerializableRunnable roleAPlayer = new CacheSerializableRunnable(
	    "ROLEAPLAYER") {
	      public void run2() throws CacheException
	      {
	        
	        Properties props = new Properties();
	        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
	        props.setProperty(DistributionConfig.ROLES_NAME, rr1);

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
	    assertEquals(stats.getReliableRegionsMissing(), (stats.getReliableRegionsMissingNoAccess() + 
	    		stats.getReliableRegionsMissingLimitedAccess() + stats.getReliableRegionsMissingFullAccess() ));
		
		
	}
	

}
