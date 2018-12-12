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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.cache.InternalCache;

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

  /** The ids of the cache servers associated with this cache */
  private int[] bridgeServerIds;

  /** Is this is a cache server? */
  private boolean isServer;

  public RemoteCacheInfo(InternalCache internalCache) {
    this.name = internalCache.getName();
    this.id = System.identityHashCode(internalCache);
    this.closed = internalCache.isClosed();
    this.lockTimeout = internalCache.getLockTimeout();
    this.lockLease = internalCache.getLockLease();
    this.searchTimeout = internalCache.getSearchTimeout();
    this.upTime = internalCache.getUpTime();
    if (this.closed) {
      this.rootRegionNames = null;
      this.perfStats = null;
      this.bridgeServerIds = new int[0];

    } else {
      try {
        final Set roots;
        if (!Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "PRDebug")) {
          roots = internalCache.rootRegions();
        } else {
          roots = internalCache.rootRegions(true);
        }

        String[] rootNames = new String[roots.size()];
        int idx = 0;
        for (Object root : roots) {
          Region r = (Region) root;
          rootNames[idx] = r.getName();
          idx++;
        }
        this.rootRegionNames = rootNames;
      } catch (CacheRuntimeException ignore) {
        this.rootRegionNames = null;
      }
      this.perfStats = new RemoteStatResource(internalCache.getCachePerfStats().getStats());

      // Note that since this is only a snapshot, so no synchronization
      // on allBridgeServersLock is needed.
      Collection<CacheServer> bridges = internalCache.getCacheServers();
      this.bridgeServerIds = new int[bridges.size()];
      Iterator<CacheServer> iter = bridges.iterator();
      for (int i = 0; iter.hasNext(); i++) {
        CacheServer bridge = iter.next();
        this.bridgeServerIds[i] = System.identityHashCode(bridge);
      }

      this.isServer = internalCache.isServer();
    }
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteCacheInfo() {
    // do nothing
  }

  @Override
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

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.id = in.readInt();
    this.closed = in.readBoolean();
    this.lockTimeout = in.readInt();
    this.lockLease = in.readInt();
    this.searchTimeout = in.readInt();
    this.upTime = in.readInt();
    this.rootRegionNames = DataSerializer.readStringArray(in);
    this.perfStats = (RemoteStatResource) DataSerializer.readObject(in);
    this.bridgeServerIds = DataSerializer.readIntArray(in);
    this.isServer = in.readBoolean();
  }

  // CacheInfo interface methods
  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public int getId() {
    return this.id;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public int getLockTimeout() {
    return this.lockTimeout;
  }

  @Override
  public int getLockLease() {
    return this.lockLease;
  }

  @Override
  public int getSearchTimeout() {
    return this.searchTimeout;
  }

  @Override
  public int getUpTime() {
    return this.upTime;
  }

  @Override
  public synchronized Set getRootRegionNames() {
    if (this.rootRegionNames == null) {
      return null;
    } else {
      return new TreeSet(Arrays.asList(this.rootRegionNames));
    }
  }

  @Override
  public StatResource getPerfStats() {
    return this.perfStats;
  }

  @Override
  public synchronized void setClosed() {
    this.closed = true;
    this.rootRegionNames = null;
  }

  @Override
  public int[] getBridgeServerIds() {
    return this.bridgeServerIds;
  }

  @Override
  public boolean isServer() {
    return this.isServer;
  }

  // other instance methods

  void setGemFireVM(RemoteGemFireVM vm) {
    if (this.perfStats != null) {
      this.perfStats.setGemFireVM(vm);
    }
  }

  @Override
  public String toString() {
    return String.format("Information about the cache %s with %s cache servers",
        this.name, this.bridgeServerIds.length);
  }
}
