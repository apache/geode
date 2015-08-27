/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.admin.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.*;

/**
 * This class is an implementation of the {@link CacheInfo} interface.
 */
public class RemoteCacheInfo implements CacheInfo, DataSerializable {
  private static final long serialVersionUID = 4251279100323876440L;

  private String name;
  private int id;
  private boolean closed;
  private int lockTimeout;
  private int lockLease;
  private int searchTimeout;
  private int upTime;
  private String[] rootRegionNames;
  private RemoteStatResource perfStats;

  /** The ids of the bridge servers associated with this cache */
  private int[] bridgeServerIds;

  /** Is this is a cache server? */
  private boolean isServer;

  public RemoteCacheInfo(GemFireCacheImpl c) {
    this.name = c.getName();
    this.id = System.identityHashCode(c);
    this.closed = c.isClosed();
    this.lockTimeout = c.getLockTimeout();
    this.lockLease = c.getLockLease();
    this.searchTimeout = c.getSearchTimeout();
    this.upTime = c.getUpTime();
    if (this.closed) {
      this.rootRegionNames = null;
      this.perfStats = null;
      this.bridgeServerIds = new int[0];

    } else {
      try {
        final Set roots;
        if (!Boolean.getBoolean("gemfire.PRDebug"))  {
          roots = c.rootRegions();
        } else {
          roots = c.rootRegions(true);
        }
        
        String[] rootNames = new String[roots.size()];
        int idx = 0;
        Iterator it = roots.iterator();
        while (it.hasNext()) {
          Region r = (Region)it.next();
          rootNames[idx] = r.getName();
          idx++;
        }
        this.rootRegionNames = rootNames;
      } catch (CacheRuntimeException ignore) {
        this.rootRegionNames = null;
      }
      this.perfStats = new RemoteStatResource(c.getCachePerfStats().getStats());

      // Note that since this is only a snapshot, so no synchronization
      // on allBridgeServersLock is needed.
      Collection bridges = c.getCacheServers();
      this.bridgeServerIds = new int[bridges.size()];
      Iterator iter = bridges.iterator();
      for (int i = 0; iter.hasNext(); i++) {
        CacheServer bridge = (CacheServer) iter.next();
        this.bridgeServerIds[i] = System.identityHashCode(bridge);
      }

      this.isServer = c.isServer();
    }
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteCacheInfo() {}


  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    out.writeInt(this.id);
    out.writeBoolean(this.closed);
    out.writeInt(this.lockTimeout);
    out.writeInt(this.lockLease);
    out.writeInt(this.searchTimeout);
    out.writeInt(this.upTime);
    DataSerializer.writeStringArray(this.rootRegionNames, out);
    DataSerializer.writeObject(this.perfStats, out);
    DataSerializer.writeIntArray(this.bridgeServerIds, out);
    out.writeBoolean(this.isServer);
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.id = in.readInt();
    this.closed = in.readBoolean();
    this.lockTimeout = in.readInt();
    this.lockLease = in.readInt();
    this.searchTimeout = in.readInt();
    this.upTime = in.readInt();
    this.rootRegionNames = DataSerializer.readStringArray(in);
    this.perfStats = (RemoteStatResource)DataSerializer.readObject(in);
    this.bridgeServerIds = DataSerializer.readIntArray(in);
    this.isServer = in.readBoolean();
  }

  // CacheInfo interface methods
  public String getName() {
    return this.name;
  }
  public int getId() {
    return this.id;
  }
  public boolean isClosed() {
    return this.closed;
  }
  public int getLockTimeout() {
    return this.lockTimeout;
  }
  public int getLockLease() {
    return this.lockLease;
  }
  public int getSearchTimeout() {
    return this.searchTimeout;
  }
  public int getUpTime() {
    return this.upTime;
  }
  public synchronized Set getRootRegionNames() {
    if (this.rootRegionNames == null) {
      return null;
    } else {
      return new TreeSet(Arrays.asList(this.rootRegionNames));
    }
  }
  public StatResource getPerfStats() {
    return this.perfStats;
  }
  public synchronized void setClosed() {
    this.closed = true;
    this.rootRegionNames = null;
  }
  public int[] getBridgeServerIds() {
    return this.bridgeServerIds;
  }
  public boolean isServer() {
    return this.isServer;
  }

  // other instance methods

  void setGemFireVM( RemoteGemFireVM vm ){
    if (this.perfStats != null) {
      this.perfStats.setGemFireVM(vm);
    }
  }

  @Override
  public String toString() {
    return LocalizedStrings.RemoteCacheInfo_INFORMATION_ABOUT_THE_CACHE_0_WITH_1_BRIDGE_SERVERS.toLocalizedString(new Object[] { this.name, Integer.valueOf(this.bridgeServerIds.length)});
  }
}
