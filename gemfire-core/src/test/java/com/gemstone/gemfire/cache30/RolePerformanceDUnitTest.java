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
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Tests the performance of Regions when Roles are assigned.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class RolePerformanceDUnitTest extends CacheTestCase {

  public RolePerformanceDUnitTest(String name) {
    super(name);
  }

  /**
   * Compares times required for series of puts with Roles assigned to
   * series of puts with no Roles assigned. Scope is D_ACK.
   * <p>
   * Up to 10 attempts will be made before failing.
   */
  public void testRolePerformance() {
    int maxAttempts = 10;
    for (int i = 1; i <= maxAttempts; i++) {
      try {
        if (i > 1) {
          // clean up from previous run
          closeCaches();
        }
        doTestRolePerformance();
        break;
      }
      // only catch assertion failures...
      catch (junit.framework.AssertionFailedError e) {
        if (i == maxAttempts) {
          throw e;
        }
        else {
          getLogWriter().info("testRolePerformance attempt #" + i + 
            " failed -- reattempting up to 10x", e);
        }
      }
    }
  }
  
  /**
   * Implementation of testRolePerformance.
   */
  private void doTestRolePerformance() {
    final String name = this.getUniqueName();

    // throw away this run
    createConnections(name, false);
    createRegions(name);
    executeOperations(name);
    
    closeCaches();
    
    // first time with no roles
    createConnections(name, false);
    createRegions(name);
    long millisNoRoles = executeOperations(name);
    
    closeCaches();
    
    // second time with roles
    createConnections(name, true);
    createRegions(name);
    long millisWithRoles = executeOperations(name);
    
    long deviation = (long)(millisNoRoles * 0.05); // 5% increase is allowed
    long ceiling = millisNoRoles + deviation;

    String data = name + " results: millisNoRoles=" + millisNoRoles +
      ", millisWithRoles=" + millisWithRoles + ", deviation=" + deviation + 
      ", ceiling=" + ceiling;
    getLogWriter().info(data);
    
    assertTrue("millisWithRoles is greater than allowable deviation: " + data,
               millisWithRoles <= ceiling);
  }
  
  /**
   * Create connection to distributed system in all vms and assign one role
   * to each if assignRoles is true.
   */
  private void createConnections(final String name, final boolean assignRoles) {
    final String[][] vmRoles = new String[][] 
      {{name+"-A"},{name+"-B"},{name+"-C"},{name+"-D"}};
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable("Connect") {
        public void run() {
          Properties config = new Properties();
          if (assignRoles) {
            config.setProperty(DistributionConfig.ROLES_NAME, vmRoles[vm][0]);
          }
          getSystem(config);
        }
      });
    }
  }
  
  /**
   * Close the cache in all vms.
   */
  private void closeCaches() { 
    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new CacheSerializableRunnable("Close Cache") {
        public void run2() throws CacheException {
          closeCache();
        }
      });
    }
  }
  
  /**
   * Create the named root region in all vms.
   */
  private void createRegions(final String name) {
    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new CacheSerializableRunnable("Create Region") {
        public void run2() throws CacheException {
          AttributesFactory fac = new AttributesFactory();
          fac.setScope(Scope.DISTRIBUTED_ACK);
          fac.setDataPolicy(DataPolicy.REPLICATE);
          RegionAttributes attr = fac.create();
          createRootRegion(name, attr);
        }
      });
    }
  }
  
  /**
   * Execute operations on the named region in one vm.
   */
  private long executeOperations(final String name) {
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Operations") {
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
          region.put("KEY-"+i, "VAL-"+i);
        }
        long finish = System.currentTimeMillis();
        timing = finish - begin;
      }
    });
    Long timing = (Long) Host.getHost(0).getVM(0).invoke(
      RolePerformanceDUnitTest.class, "getTiming", new Object[] {});
    return timing.longValue();
  }
  protected static transient long timing = -1;
  private static Long getTiming() {
    return new Long(timing);
  }
  
}

