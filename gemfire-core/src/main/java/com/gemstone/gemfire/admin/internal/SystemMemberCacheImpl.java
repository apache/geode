/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ObjIdMap;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * View of a GemFire system member's cache.
 *
 * @author    Darrel Schneider
 * @since     3.5
 */
public class SystemMemberCacheImpl implements SystemMemberCache {
  protected final GemFireVM vm;
  protected CacheInfo info;
  protected Statistic[] statistics;

  /** Maps the id of a bridge server to its SystemMemberBridgeServer */ 
  private ObjIdMap bridgeServers = new ObjIdMap();
  
  // constructors
  public SystemMemberCacheImpl(GemFireVM vm)
    throws CacheDoesNotExistException
  {
    this.vm = vm;
    this.info = vm.getCacheInfo();
    if (this.info == null) {
      throw new CacheDoesNotExistException(LocalizedStrings.SystemMemberCacheImpl_THE_VM_0_DOES_NOT_CURRENTLY_HAVE_A_CACHE.toLocalizedString(vm.getId()));
    }
    initStats();
  }
  
  // attributes
  /**
   * The name of the cache.
   */
  public String getName() {
    String result = this.info.getName();
    if (result == null || result.length() == 0) {
      result = "default";
    }
    return result;
  }
  /**
   * Value that uniquely identifies an instance of a cache for a given member.
   */
  public int getId() {
    return this.info.getId();
  }

  public boolean isClosed() {
    return this.info.isClosed();
  }
  public int getLockTimeout() {
    return this.info.getLockTimeout();
  }
  public void setLockTimeout(int seconds) throws AdminException {
    this.info = this.vm.setCacheLockTimeout(this.info, seconds);
  }
  public int getLockLease() {
    return this.info.getLockLease();
  }
  public void setLockLease(int seconds) throws AdminException {
    this.info = this.vm.setCacheLockLease(this.info, seconds);
  }
  public int getSearchTimeout() {
    return this.info.getSearchTimeout();
  }
  public void setSearchTimeout(int seconds) throws AdminException {
    this.info = this.vm.setCacheSearchTimeout(this.info, seconds);
  }
  public int getUpTime() {
    return this.info.getUpTime();
  }
  public java.util.Set getRootRegionNames() {
    Set set = this.info.getRootRegionNames();
    if (set == null) {
      set = Collections.EMPTY_SET;
    }
    return set;
  }
  // operations

  public void refresh() {
    if (!this.info.isClosed()) {
      CacheInfo cur = vm.getCacheInfo();
      if (cur == null || (this.info.getId() != cur.getId())) {
        // it is a different instance of the cache. So set our version
        // to closed
        this.info.setClosed();
      } else {
        this.info = cur;
        updateStats();
      }
    }
  }

  public GemFireMemberStatus getSnapshot()
  {
	  //System.out.println(">>>SystemMemberCacheJmxImpl::getSnapshot:pre::: " + this.vm);
	  GemFireMemberStatus stat = this.vm.getSnapshot();
	  //System.out.println(">>>SystemMemberCacheJmxImpl::getSnapshot:post::: " + stat);
	  return stat;
  }

  public RegionSubRegionSnapshot getRegionSnapshot()
  {
	  //System.out.println(">>>SystemMemberCacheJmxImpl::getRegionSnapshot:pre::: " + this.vm);
	  RegionSubRegionSnapshot snap = this.vm.getRegionSnapshot();
	  //System.out.println(">>>SystemMemberCacheJmxImpl::getRegionSnapshot:post::: " + snap);
	  return snap;
  }
  
  public Statistic[] getStatistics() {
    return this.statistics;
  }

  public SystemMemberRegion getRegion(String path)
    throws com.gemstone.gemfire.admin.AdminException
  {
    Region r = this.vm.getRegion(this.info, path);
    if (r == null) {
      return null;
    } else {
      return createSystemMemberRegion(r);
    }
  }

  public SystemMemberRegion createRegion(String name,
                                         RegionAttributes attrs)
    throws AdminException
  {
    Region r = this.vm.createVMRootRegion(this.info, name, attrs);
    if (r == null) {
      return null;

    } else {
      return createSystemMemberRegion(r);
    }
  }
  
  public SystemMemberRegion createVMRegion(String name,
                                           RegionAttributes attrs)
    throws AdminException
  {
    return createRegion(name, attrs);
  }


  // internal methods
  private void initStats() {
    StatResource resource = this.info.getPerfStats();
    if (resource == null) {
      // See bug 31397
      Assert.assertTrue(this.isClosed());
      return;
    }

    Stat[] stats = resource.getStats();
    if (stats == null || stats.length < 1) {
      this.statistics = new Statistic[0];
      return;
    }
    
    // define new statistics instances...
    List statList = new ArrayList();
    for (int i = 0; i < stats.length; i++) {
      statList.add(createStatistic(stats[i]));
    }
    this.statistics = (Statistic[]) statList.toArray(new Statistic[statList.size()]);
  }
  private void updateStats() {
    StatResource resource = this.info.getPerfStats();
    if (resource == null) {
      // See bug 31397
      Assert.assertTrue(this.isClosed());
      return;
    }

    Stat[] stats = resource.getStats();
    if (stats == null || stats.length < 1) {
      return;
    }
    
    for (int i = 0; i < stats.length; i++) {
      updateStatistic(stats[i]);
    }
  }

  private void updateStatistic(Stat stat) {
    for (int i = 0; i < this.statistics.length; i++) {
      if (this.statistics[i].getName().equals(stat.getName())) {
        ((StatisticImpl)this.statistics[i]).setStat(stat);
        return;
      }
    }
    Assert.assertTrue(false, "Unknown stat: " + stat.getName());
  }

  /**
   * Returns the <code>CacheInfo</code> that describes this cache.
   * Note that this operation does not {@link #refresh} the
   * <code>CacheInfo</code>. 
   */
  public CacheInfo getCacheInfo() {
    return this.info;
  }

  public GemFireVM getVM() {
    return this.vm;
  }

  protected Statistic createStatistic(Stat stat) {
    return new StatisticImpl(stat);
  }
  protected SystemMemberRegion createSystemMemberRegion(Region r)
    throws com.gemstone.gemfire.admin.AdminException
  {
    SystemMemberRegionImpl sysMemberRegion = new SystemMemberRegionImpl(this, r);
    sysMemberRegion.refresh();
    return sysMemberRegion;
  }

  public SystemMemberCacheServer addCacheServer()
    throws AdminException {

    AdminBridgeServer bridge = this.vm.addCacheServer(this.info);
    SystemMemberCacheServer admin =
      createSystemMemberBridgeServer(bridge);
    bridgeServers.put(bridge.getId(), admin);
    return admin;
  }

  private Collection getCacheServersCollection()
    throws AdminException {
    Collection bridges = new ArrayList();

    int[] bridgeIds = this.info.getBridgeServerIds();
    for (int i = 0; i < bridgeIds.length; i++) {
      int id = bridgeIds[i];
      SystemMemberBridgeServer bridge =
        (SystemMemberBridgeServer) bridgeServers.get(id);
      if (bridge == null) {
        AdminBridgeServer info = this.vm.getBridgeInfo(this.info, id);
        if (info != null) {
          bridge = createSystemMemberBridgeServer(info);
          bridgeServers.put(info.getId(), bridge);
        }
      }

      if (bridge != null) {
        bridges.add(bridge);
      }
    }
    return bridges;
  }

  public SystemMemberCacheServer[] getCacheServers()
    throws AdminException {
    Collection bridges = getCacheServersCollection();
    SystemMemberCacheServer[] array =
      new SystemMemberCacheServer[bridges.size()];
    return (SystemMemberCacheServer[]) bridges.toArray(array);
  };

  /**
   * Creates a new instance of <Code>SystemMemberBridgeServer</code>
   * with the given configuration.
   */
  protected SystemMemberBridgeServerImpl
    createSystemMemberBridgeServer(AdminBridgeServer bridge) 
    throws AdminException {

    return new SystemMemberBridgeServerImpl(this, bridge);
  }

  public boolean isServer() throws AdminException {
    return this.info.isServer();
  }
  
  
  /**
   * Returns a string representation of the object.
   * 
   * @return a string representation of the object
   */
  @Override
  public String toString() {
	return getName();
  }
}

