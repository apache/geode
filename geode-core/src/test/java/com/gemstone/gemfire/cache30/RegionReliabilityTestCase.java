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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitDistributionException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAccessException;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDistributionException;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.cache.RegionReinitializedException;
import com.gemstone.gemfire.cache.RequiredRoles;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.util.RegionMembershipListenerAdapter;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.TXStateProxyImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Tests region reliability defined by MembershipAttributes.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class RegionReliabilityTestCase extends ReliabilityTestCase {

  public RegionReliabilityTestCase(String name) {
    super(name);
  }

  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    DistributedCacheOperation.setBeforePutOutgoing(null);
  }

  // -------------------------------------------------------------------------
  // Configuration and setup methods
  // -------------------------------------------------------------------------
  
  /** Returns scope to execute tests under. */
  protected abstract Scope getRegionScope();
  
  protected InternalDistributedSystem createConnection(String[] roles) {
    StringBuffer rolesValue = new StringBuffer();
    if (roles != null) {
      for (int i = 0; i < roles.length; i++) {
        if (i > 0) {
          rolesValue.append(",");
        }
        rolesValue.append(roles[i]);
      }
    }
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, rolesValue.toString());
    return getSystem(config);
  }

  protected void assertLimitedAccessThrows(Region region) throws Exception {
    try {
      region.clear();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.create("KEY", "VAL");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.destroy(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.destroyRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    if (region.getAttributes().getScope().isGlobal()) {
      try {
        region.becomeLockGrantor();
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      try {
        region.getDistributedLock(new Object());
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      try {
        region.getRegionDistributedLock();
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
    }
    try {
      region.invalidate(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.invalidateRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.loadSnapshot(new ByteArrayInputStream(new byte[] {}));
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try { // netload TODO: configure CacheLoader in region
      region.get("netload");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try { // netsearch TODO: add 2nd VM that has the object
      region.get("netsearch");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.put(new Object(), new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      Map map = new HashMap();
      map.put(new Object(), new Object());
      region.putAll(map);
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      Map map = new HashMap();
      map.put(new Object(), new Object());
      region.putAll(map, "callbackArg");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.remove(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    if (!region.getAttributes().getScope().isGlobal()) {
      CacheTransactionManager tx = region.getCache().getCacheTransactionManager();
      tx.begin();
      try {
        region.put("KEY-tx", "VAL-tx");
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      tx.rollback();
    }
  }
  
  protected void assertNoAccessThrows(Region region) throws Exception {
    assertLimitedAccessThrows(region);
    try {
      region.containsKey(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.containsValue(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.containsValueForKey(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.entries(false);
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.entrySet();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.get(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.getEntry(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.isEmpty();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.keys();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.keySet();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localDestroy(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localInvalidate(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localInvalidateRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.saveSnapshot(new ByteArrayOutputStream());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.size();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.values();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    
    try {
      QueryService qs = region.getCache().getQueryService();
      Query query = qs.newQuery(
        "(select distinct * from " + region.getFullPath() + ").size");
      query.execute();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
  }
  
  protected void assertLimitedAccessDoesNotThrow(Region region) throws Exception {
    // insert some values for test
    Object[] keys = new Object[] {"hip", "hop"};
    Object[] values = new Object[] {"clip", "clop"};
    for (int i = 0; i < keys.length; i++) {
      region.put(keys[i], values[i]);
    }

    // test the ops that can throw RegionAccessException for LIMITED_ACCESS
    region.create("jack", "jill");
    region.destroy("jack");
    
    if (region.getAttributes().getScope().isGlobal()) {
      region.becomeLockGrantor();

      Lock dlock = region.getDistributedLock(keys[0]);
      dlock.lock();
      dlock.unlock();
      
      Lock rlock = region.getRegionDistributedLock();
      rlock.lock();
      rlock.unlock();
    }
    
    // netload (configured in vm0)
    assertEquals("netload", region.get("netload"));
    // netsearch (entry exists in vm0)
    assertEquals("netsearch", region.get("netsearch"));
    
    region.invalidate(keys[0]);
    region.invalidateRegion();
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    region.saveSnapshot(baos);
    region.loadSnapshot(new ByteArrayInputStream(baos.toByteArray()));
    
    // need to get a new handle to the region...
    region = getRootRegion(region.getFullPath());
    
    region.put(keys[0], values[0]);

    Map map = new HashMap();
    map.put("mom", "pop");
    region.putAll(map);
    region.putAll(map, "callbackArg");

    QueryService qs = region.getCache().getQueryService();
    Query query = qs.newQuery(
      "(select distinct * from " + region.getFullPath() + ").size");
    query.execute();
    
    region.remove(keys[0]);

    if (!region.getAttributes().getScope().isGlobal()) {
      CacheTransactionManager tx = region.getCache().getCacheTransactionManager();
      tx.begin();
      region.put("KEY-tx", "VAL-tx");
      tx.commit();
    }
    
    region.clear();
    region.destroyRegion();
  }
  
  protected void assertNoAccessDoesNotThrow(Region region) throws Exception {
    // insert some values for test
    Object[] keys = new Object[] {"bip", "bam"};
    Object[] values = new Object[] {"foo", "bar"};
    for (int i = 0; i < keys.length; i++) {
      region.put(keys[i], values[i]);
    }
    
    // test the ops that can throw RegionAccessException for NO_ACCESS
    region.containsKey(new Object());
    region.containsValue(new Object());
    region.containsValueForKey(new Object());
    region.entries(false);
    region.entrySet();
    region.get(keys[0]);
    region.getEntry(keys[0]);
    region.isEmpty();
    region.keys();
    region.keySet();
    region.localDestroy(keys[0]);
    region.localInvalidate(keys[1]);
    region.localInvalidateRegion();
    region.saveSnapshot(new ByteArrayOutputStream());
    region.size();
    region.values();

    QueryService qs = region.getCache().getQueryService();
    Query query = qs.newQuery(
      "(select distinct * from " + region.getFullPath() + ").size");
    query.execute();
    
    assertLimitedAccessDoesNotThrow(region);
  }
  
  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    }
    catch (InterruptedException e) {
      fail("interrupted");
    }
  }
  
  // -------------------------------------------------------------------------
  // Tests to be run under every permutation of config options
  //   Valid configurations include scope D-ACK, D-NOACK, GLOBAL
  // -------------------------------------------------------------------------
  
  /**
   * Tests affect of NO_ACCESS on region operations.
   */
  public void testNoAccess() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(null);
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }
          public void close() {}
        });
        RegionAttributes attr = fac.create();
        Region region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });
    
    // test ops on Region that should throw
    assertNoAccessThrows(region);
    
    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    Role role = (Role) requiredRolesSet.iterator().next();
    assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    
    // retest ops on Region to assert no longer throw    
    assertNoAccessDoesNotThrow(region);
  }
  
  private static InternalDistributedMember findDistributedMember() {
    DM dm = (
      InternalDistributedSystem.getAnyInstance()).getDistributionManager();
    return dm.getDistributionManagerId();
  }
  
  /**
   * Tests affect of NO_ACCESS on local entry expiration actions.
   */
  public void testNoAccessWithLocalEntryExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    // test to make sure expiration is not suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Close Region") {
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.close();
      }
    });
    //TODO: waitForMemberTimeout(); ?

    // set expiration and sleep
    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
    sleep(200);

    // make sure no values were expired
    Set entries = ((LocalRegion) region).basicEntries(false);
    assertTrue(entries.size() == 1);
    
    // create region again in vm1
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    waitForEntryDestroy(region, "expireMe");
  }
  
  /**
   * Tests affect of NO_ACCESS on local region expiration actions.
   */
  public void testNoAccessWithLocalRegionExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
      
    // sleep and make sure region does not expire
    sleep(200);
    assertFalse(region.isDestroyed());
    
    // create region in vm1
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    waitForRegionDestroy(region);
  }
  
  /**
   * Tests affect of LIMITED_ACCESS on region operations.
   */
  public void testLimitedAccess() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(null);
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }
          public void close() {}
        });
        RegionAttributes attr = fac.create();
        Region region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });
    
    // test ops on Region that should throw
    assertLimitedAccessThrows(region);
    
    // this query should not throw
    QueryService qs = region.getCache().getQueryService();
    Query query = qs.newQuery(
      "(select distinct * from " + region.getFullPath() + ").size");
    query.execute();
    
    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    // retest ops on Region to assert no longer throw    
    assertLimitedAccessDoesNotThrow(region);
  }
  
  /**
   * Tests affect of LIMITED_ACCESS on local entry expiration actions.
   */
  public void testLimitedAccessWithLocalEntryExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
     
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    // test to make sure expiration is suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Close Region") {
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.close();
      }
    });
    //TODO: waitForMemberTimeout(); ?

    // set expiration and sleep
    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
    WaitCriterion wc1 = new WaitCriterion() {
      public boolean done() {
        return ((LocalRegion) region).basicEntries(false).size() == 0;
      }
      public String description() {
        return "expected zero entries but have " + ((LocalRegion) region).basicEntries(false).size();
      }
    };
    Wait.waitForCriterion(wc1, 30*1000, 10, true);

    // create region again
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    region.put("expireMe", "expireMe");
    
    waitForEntryDestroy(region, "expireMe");
    assertTrue(region.size() == 0);
  }
  
  /**
   * Tests affect of LIMITED_ACCESS on local region expiration actions.
   */
  public void testLimitedAccessWithLocalRegionExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
      
    waitForRegionDestroy(region);
  }
  
  /**
   * Tests affect of FULL_ACCESS on region operations.
   */
  public void testFullAccess() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(null);
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }
          public void close() {}
        });
        RegionAttributes attr = fac.create();
        Region region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });
    
    // test ops on Region that should not throw
    assertNoAccessDoesNotThrow(region);
  }
  
  /**
   * Tests affect of FULL_ACCESS on local entry expiration actions.
   */
  public void testFullAccessWithLocalEntryExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // test to make sure expiration is not suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);

    // set expiration and sleep
    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));

    waitForEntryDestroy(region, "expireMe");
    assertTrue(region.size() == 0);
  }
  
  public static void waitForRegionDestroy(final Region region) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return region.isDestroyed();
      }
      public String description() {
        return "expected region " + region + " to be destroyed";
      }
    };
    Wait.waitForCriterion(wc, 30*1000, 10, true);
  }
  
  public static void waitForEntryDestroy(final Region region, final Object key) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return region.get(key) == null;
      }
      public String description() {
        return "expected entry " + key + " to not exist but it has the value " + region.get(key);
      }
    };
    Wait.waitForCriterion(wc, 30*1000, 10, true);
  }
  
  /**
   * Tests affect of FULL_ACCESS on local region expiration actions.
   */
  public void testFullAccessWithLocalRegionExpiration() throws Exception {
    final String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    RegionAttributes attr = fac.create();
    final Region region = createExpiryRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();

    AttributesMutator mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(
      new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
      
    waitForRegionDestroy(region);
  }
  
  protected static Boolean[] detectedDeparture_testCommitDistributionException = 
    { Boolean.FALSE };
  public void testCommitDistributionException() throws Exception {
    if (getRegionScope().isGlobal()) return; // skip test under Global
    if (getRegionScope().isDistributedNoAck()) return; // skip test under DistributedNoAck
    
    final String name = this.getUniqueName();
    final String roleA = name+"-A";
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    
    RegionMembershipListener listener = new RegionMembershipListenerAdapter() {
      public void afterRemoteRegionDeparture(RegionEvent event) {
        synchronized (detectedDeparture_testCommitDistributionException) {
          detectedDeparture_testCommitDistributionException[0] = Boolean.TRUE;
          detectedDeparture_testCommitDistributionException.notify();
        }
      }
    };
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.addCacheListener(listener);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    // define the afterReleaseLocalLocks callback
    Runnable removeRequiredRole = new SerializableRunnable() {
      public void run() {
        Host.getHost(0).getVM(1).invoke(new SerializableRunnable("Close Region") {
          public void run() {
            getRootRegion(name).close();
          }
        });
        try {
          synchronized (detectedDeparture_testCommitDistributionException) {
            while (detectedDeparture_testCommitDistributionException[0] == Boolean.FALSE) {
              detectedDeparture_testCommitDistributionException.wait();
            }
          }
        }
        catch (InterruptedException e) {fail("interrupted");}
      }
    };
    
    // define the add and remove expected exceptions
    final String expectedExceptions = 
      "com.gemstone.gemfire.internal.cache.CommitReplyException";
    SerializableRunnable addExpectedExceptions = 
      new CacheSerializableRunnable("addExpectedExceptions") {
        public void run2() throws CacheException {
          getCache().getLogger().info("<ExpectedException action=add>" + 
              expectedExceptions + "</ExpectedException>");
        }
      };
    SerializableRunnable removeExpectedExceptions = 
      new CacheSerializableRunnable("removeExpectedExceptions") {
        public void run2() throws CacheException {
          getCache().getLogger().info("<ExpectedException action=remove>" + 
              expectedExceptions + "</ExpectedException>");
        }
      };

    // perform the actual test...
      
    CacheTransactionManager ctm = cache.getCacheTransactionManager();
    ctm.begin();
    TXStateInterface txStateProxy = ((TXManagerImpl)ctm).getTXState();
    ((TXStateProxyImpl)txStateProxy).forceLocalBootstrap();
    TXState txState = (TXState)((TXStateProxyImpl)txStateProxy).getRealDeal(null,null);
    txState.setBeforeSend(removeRequiredRole);
    
    // now start a transaction and commit it
    region.put("KEY", "VAL");
      
    addExpectedExceptions.run();
    Host.getHost(0).getVM(1).invoke(addExpectedExceptions);
    
    try {
      ctm.commit();
      fail("Should have thrown CommitDistributionException");
    }
    catch (CommitDistributionException e) {
      // pass
    }
    finally {
      removeExpectedExceptions.run();
      Host.getHost(0).getVM(1).invoke(removeExpectedExceptions);
    }
  }
  
  protected static Boolean[] detectedDeparture_testRegionDistributionException = 
    { Boolean.FALSE };
  public void testRegionDistributionException() throws Exception {
    if (getRegionScope().isDistributedNoAck()) return; // skip test under DistributedNoAck
    
    final String name = this.getUniqueName();
    final String roleA = name+"-A";
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    RegionMembershipListener listener = new RegionMembershipListenerAdapter() {
      public void afterRemoteRegionDeparture(RegionEvent event) {
        synchronized (detectedDeparture_testRegionDistributionException) {
          detectedDeparture_testRegionDistributionException[0] = Boolean.TRUE;
          detectedDeparture_testRegionDistributionException.notify();
        }
      }
    };

    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
//    fac.addCacheListener(listener);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    assertTrue(((AbstractRegion)region).requiresReliabilityCheck());
    
    // use vm1 to create role
    CacheSerializableRunnable createRegion = new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    };
    
    Host.getHost(0).getVM(1).invoke(createRegion);
    region.put("DESTROY_ME", "VAL");
    region.put("INVALIDATE_ME", "VAL");
      
    // define the afterReleaseLocalLocks callback
    Runnable removeRequiredRole = new SerializableRunnable() {
      public void run() {
        Host.getHost(0).getVM(1).invoke(new SerializableRunnable("Close Region") {
          public void run() {
            getRootRegion(name).close();
          }
        });
//        try {
//          synchronized (detectedDeparture_testRegionDistributionException) {
//            while (detectedDeparture_testRegionDistributionException[0] == Boolean.FALSE) {
//              detectedDeparture_testRegionDistributionException.wait();
//            }
//          }
//        }
//        catch (InterruptedException e) {}
      }
    };
    DistributedCacheOperation.setBeforePutOutgoing(removeRequiredRole);

    Runnable reset = new Runnable() {
      public void run() {
//        synchronized (detectedDeparture_testRegionDistributionException) {
//          detectedDeparture_testRegionDistributionException[0] = Boolean.FALSE;
//        }
      }
    };
    
    // PUT    
    try {
      region.put("KEY", "VAL");
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
    
    // INVALIDATE
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.invalidate("INVALIDATE_ME");
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
    
    // DESTROY
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.destroy("DESTROY_ME");
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }

    // CLEAR
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.clear();
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
    
    // PUTALL
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      Map putAll = new HashMap();
      putAll.put("PUTALL_ME", "VAL");
      region.putAll(putAll);
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
    
    // INVALIDATE REGION
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.invalidateRegion();
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
    
    // DESTROY REGION
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.destroyRegion();
      fail("Should have thrown RegionDistributionException");
    }
    catch (RegionDistributionException e) {
      // pass
    }
  }

  public void testReinitialization() throws Exception {
    final String name = this.getUniqueName();
    final String roleA = name+"-A";
    final String[] requiredRoles = {roleA};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    getCache();
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.NO_ACCESS, ResumptionAction.REINITIALIZE);
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    assertTrue(((AbstractRegion)region).requiresReliabilityCheck());
    assertFalse(RequiredRoles.checkForRequiredRoles(region).isEmpty());

    final String key = "KEY-testReinitialization";
    final String val = "VALUE-testReinitialization";
    
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Data") {
      public void run2() throws CacheException {
        createConnection(new String[] {});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setDataPolicy(DataPolicy.REPLICATE);
        RegionAttributes attr = fac.create();
        Region region = createRootRegion(name, attr);
        region.put(key, val);
      }
    });

    final Region finalRegion = region;
    Thread thread = new Thread(new Runnable() {
      public void run() {
        try {
          RequiredRoles.waitForRequiredRoles(finalRegion, -1);
        } 
        catch (InterruptedException e) {fail("interrupted");}
        catch (RegionReinitializedException e) {}
      }
    });
    thread.start();
    
    // create role and verify reinitialization took place
    Host.getHost(0).getVM(1).invokeAsync(new CacheSerializableRunnable("Create Role") {
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    });
    
    ThreadUtils.join(thread, 30 * 1000);
    assertTrue(region.isDestroyed());
    try {
      region.put("fee", "fi");
      fail("Should have thrown RegionReinitializedException");
    }
    catch(RegionReinitializedException e) {
      // pass
    }
    try {
      RequiredRoles.checkForRequiredRoles(region);
      fail("Should have thrown RegionReinitializedException");
    }
    catch(RegionReinitializedException e) {
      // pass
    }
    try {
      Role role = (Role) requiredRolesSet.iterator().next();
      RequiredRoles.isRoleInRegionMembership(region, role);
      fail("Should have thrown RegionReinitializedException");
    }
    catch(RegionReinitializedException e) {
      // pass
    }
    
    region = getRootRegion(name);
    assertNotNull(region);
    assertTrue(((AbstractRegion)region).requiresReliabilityCheck());
    assertTrue(RequiredRoles.checkForRequiredRoles(region).isEmpty());
    assertNotNull(region.getEntry(key));
    assertEquals(val, region.getEntry(key).getValue());
  }
  
}

