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
package org.apache.geode.admin.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.CacheDoesNotExistException;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.admin.Statistic;
import org.apache.geode.admin.SystemMemberBridgeServer;
import org.apache.geode.admin.SystemMemberCache;
import org.apache.geode.admin.SystemMemberCacheServer;
import org.apache.geode.admin.SystemMemberRegion;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ObjIdMap;
import org.apache.geode.internal.admin.AdminBridgeServer;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatResource;

/**
 * View of a GemFire system member's cache.
 *
 * @since GemFire 3.5
 */
public class SystemMemberCacheImpl implements SystemMemberCache {
  protected final GemFireVM vm;
  protected CacheInfo info;
  protected Statistic[] statistics;

  /** Maps the id of a cache server to its SystemMemberBridgeServer */
  private final ObjIdMap bridgeServers = new ObjIdMap();

  // constructors
  public SystemMemberCacheImpl(GemFireVM vm) throws CacheDoesNotExistException {
    this.vm = vm;
    info = vm.getCacheInfo();
    if (info == null) {
      throw new CacheDoesNotExistException(
          String.format("The VM %s does not currently have a cache.",
              vm.getId()));
    }
    initStats();
  }

  // attributes
  /**
   * The name of the cache.
   */
  @Override
  public String getName() {
    String result = info.getName();
    if (result == null || result.length() == 0) {
      result = "default";
    }
    return result;
  }

  /**
   * Value that uniquely identifies an instance of a cache for a given member.
   */
  @Override
  public int getId() {
    return info.getId();
  }

  @Override
  public boolean isClosed() {
    return info.isClosed();
  }

  @Override
  public int getLockTimeout() {
    return info.getLockTimeout();
  }

  @Override
  public void setLockTimeout(int seconds) throws AdminException {
    info = vm.setCacheLockTimeout(info, seconds);
  }

  @Override
  public int getLockLease() {
    return info.getLockLease();
  }

  @Override
  public void setLockLease(int seconds) throws AdminException {
    info = vm.setCacheLockLease(info, seconds);
  }

  @Override
  public int getSearchTimeout() {
    return info.getSearchTimeout();
  }

  @Override
  public void setSearchTimeout(int seconds) throws AdminException {
    info = vm.setCacheSearchTimeout(info, seconds);
  }

  @Override
  public int getUpTime() {
    return info.getUpTime();
  }

  @Override
  public java.util.Set getRootRegionNames() {
    Set set = info.getRootRegionNames();
    if (set == null) {
      set = Collections.EMPTY_SET;
    }
    return set;
  }
  // operations

  @Override
  public void refresh() {
    if (!info.isClosed()) {
      CacheInfo cur = vm.getCacheInfo();
      if (cur == null || (info.getId() != cur.getId())) {
        // it is a different instance of the cache. So set our version
        // to closed
        info.setClosed();
      } else {
        info = cur;
        updateStats();
      }
    }
  }

  public GemFireMemberStatus getSnapshot() {
    GemFireMemberStatus stat = vm.getSnapshot();
    return stat;
  }

  public RegionSubRegionSnapshot getRegionSnapshot() {
    RegionSubRegionSnapshot snap = vm.getRegionSnapshot();
    return snap;
  }

  @Override
  public Statistic[] getStatistics() {
    return statistics;
  }

  @Override
  public SystemMemberRegion getRegion(String path) throws org.apache.geode.admin.AdminException {
    Region r = vm.getRegion(info, path);
    if (r == null) {
      return null;
    } else {
      return createSystemMemberRegion(r);
    }
  }

  @Override
  public SystemMemberRegion createRegion(String name, RegionAttributes attrs)
      throws AdminException {
    Region r = vm.createVMRootRegion(info, name, attrs);
    if (r == null) {
      return null;

    } else {
      return createSystemMemberRegion(r);
    }
  }

  @Override
  public SystemMemberRegion createVMRegion(String name, RegionAttributes attrs)
      throws AdminException {
    return createRegion(name, attrs);
  }


  // internal methods
  private void initStats() {
    StatResource resource = info.getPerfStats();
    if (resource == null) {
      // See bug 31397
      Assert.assertTrue(isClosed());
      return;
    }

    Stat[] stats = resource.getStats();
    if (stats == null || stats.length < 1) {
      statistics = new Statistic[0];
      return;
    }

    // define new statistics instances...
    List statList = new ArrayList();
    for (int i = 0; i < stats.length; i++) {
      statList.add(createStatistic(stats[i]));
    }
    statistics = (Statistic[]) statList.toArray(new Statistic[0]);
  }

  private void updateStats() {
    StatResource resource = info.getPerfStats();
    if (resource == null) {
      // See bug 31397
      Assert.assertTrue(isClosed());
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
    for (int i = 0; i < statistics.length; i++) {
      if (statistics[i].getName().equals(stat.getName())) {
        ((StatisticImpl) statistics[i]).setStat(stat);
        return;
      }
    }
    Assert.assertTrue(false, "Unknown stat: " + stat.getName());
  }

  /**
   * Returns the <code>CacheInfo</code> that describes this cache. Note that this operation does not
   * {@link #refresh} the <code>CacheInfo</code>.
   */
  public CacheInfo getCacheInfo() {
    return info;
  }

  public GemFireVM getVM() {
    return vm;
  }

  protected Statistic createStatistic(Stat stat) {
    return new StatisticImpl(stat);
  }

  protected SystemMemberRegion createSystemMemberRegion(Region r)
      throws org.apache.geode.admin.AdminException {
    SystemMemberRegionImpl sysMemberRegion = new SystemMemberRegionImpl(this, r);
    sysMemberRegion.refresh();
    return sysMemberRegion;
  }

  @Override
  public SystemMemberCacheServer addCacheServer() throws AdminException {

    AdminBridgeServer bridge = vm.addCacheServer(info);
    SystemMemberCacheServer admin = createSystemMemberBridgeServer(bridge);
    bridgeServers.put(bridge.getId(), admin);
    return admin;
  }

  private Collection getCacheServersCollection() throws AdminException {
    Collection bridges = new ArrayList();

    int[] bridgeIds = info.getBridgeServerIds();
    for (int i = 0; i < bridgeIds.length; i++) {
      int id = bridgeIds[i];
      SystemMemberBridgeServer bridge = (SystemMemberBridgeServer) bridgeServers.get(id);
      if (bridge == null) {
        AdminBridgeServer info = vm.getBridgeInfo(this.info, id);
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

  @Override
  public SystemMemberCacheServer[] getCacheServers() throws AdminException {
    Collection bridges = getCacheServersCollection();
    SystemMemberCacheServer[] array = new SystemMemberCacheServer[bridges.size()];
    return (SystemMemberCacheServer[]) bridges.toArray(array);
  }

  /**
   * Creates a new instance of <Code>SystemMemberBridgeServer</code> with the given configuration.
   */
  protected SystemMemberBridgeServerImpl createSystemMemberBridgeServer(AdminBridgeServer bridge)
      throws AdminException {

    return new SystemMemberBridgeServerImpl(this, bridge);
  }

  @Override
  public boolean isServer() throws AdminException {
    return info.isServer();
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
