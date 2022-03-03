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
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.util.internal.GeodeGlossary;

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
    name = internalCache.getName();
    id = System.identityHashCode(internalCache);
    closed = internalCache.isClosed();
    lockTimeout = internalCache.getLockTimeout();
    lockLease = internalCache.getLockLease();
    searchTimeout = internalCache.getSearchTimeout();
    upTime = (int) internalCache.getUpTime();
    if (closed) {
      rootRegionNames = null;
      perfStats = null;
      bridgeServerIds = new int[0];

    } else {
      try {
        final Set roots;
        if (!Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "PRDebug")) {
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
        rootRegionNames = rootNames;
      } catch (CacheRuntimeException ignore) {
        rootRegionNames = null;
      }
      perfStats = new RemoteStatResource(internalCache.getCachePerfStats().getStats());

      // Note that since this is only a snapshot, so no synchronization
      // on allBridgeServersLock is needed.
      Collection<CacheServer> bridges = internalCache.getCacheServers();
      bridgeServerIds = new int[bridges.size()];
      Iterator<CacheServer> iter = bridges.iterator();
      for (int i = 0; iter.hasNext(); i++) {
        CacheServer bridge = iter.next();
        bridgeServerIds[i] = System.identityHashCode(bridge);
      }

      isServer = internalCache.isServer();
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
    DataSerializer.writeString(name, out);
    out.writeInt(id);
    out.writeBoolean(closed);
    out.writeInt(lockTimeout);
    out.writeInt(lockLease);
    out.writeInt(searchTimeout);
    out.writeInt(upTime);
    DataSerializer.writeStringArray(rootRegionNames, out);
    DataSerializer.writeObject(perfStats, out);
    DataSerializer.writeIntArray(bridgeServerIds, out);
    out.writeBoolean(isServer);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    name = DataSerializer.readString(in);
    id = in.readInt();
    closed = in.readBoolean();
    lockTimeout = in.readInt();
    lockLease = in.readInt();
    searchTimeout = in.readInt();
    upTime = in.readInt();
    rootRegionNames = DataSerializer.readStringArray(in);
    perfStats = DataSerializer.readObject(in);
    bridgeServerIds = DataSerializer.readIntArray(in);
    isServer = in.readBoolean();
  }

  // CacheInfo interface methods
  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public int getLockTimeout() {
    return lockTimeout;
  }

  @Override
  public int getLockLease() {
    return lockLease;
  }

  @Override
  public int getSearchTimeout() {
    return searchTimeout;
  }

  @Override
  public int getUpTime() {
    return upTime;
  }

  @Override
  public synchronized Set getRootRegionNames() {
    if (rootRegionNames == null) {
      return null;
    } else {
      return new TreeSet(Arrays.asList(rootRegionNames));
    }
  }

  @Override
  public StatResource getPerfStats() {
    return perfStats;
  }

  @Override
  public synchronized void setClosed() {
    closed = true;
    rootRegionNames = null;
  }

  @Override
  public int[] getBridgeServerIds() {
    return bridgeServerIds;
  }

  @Override
  public boolean isServer() {
    return isServer;
  }

  // other instance methods

  void setGemFireVM(RemoteGemFireVM vm) {
    if (perfStats != null) {
      perfStats.setGemFireVM(vm);
    }
  }

  @Override
  public String toString() {
    return String.format("Information about the cache %s with %s cache servers",
        name, bridgeServerIds.length);
  }
}
