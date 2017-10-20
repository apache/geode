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
package org.apache.geode.internal.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class OverflowOplogSet implements OplogSet {
  private static final Logger logger = LogService.getLogger();

  private final AtomicInteger overflowOplogId = new AtomicInteger(0);
  private OverflowOplog lastOverflowWrite = null;
  private final ConcurrentMap<Integer, OverflowOplog> overflowMap =
      new ConcurrentHashMap<Integer, OverflowOplog>();
  private final Map<Integer, OverflowOplog> compactableOverflowMap =
      new LinkedHashMap<Integer, OverflowOplog>();

  private int lastOverflowDir = 0;

  private DiskStoreImpl parent;

  public OverflowOplogSet(DiskStoreImpl parent) {
    this.parent = parent;
  }


  OverflowOplog getActiveOverflowOplog() {
    return this.lastOverflowWrite;
  }

  @Override
  public void modify(LocalRegion lr, DiskEntry entry, ValueWrapper value, boolean async) {
    DiskRegion dr = lr.getDiskRegion();
    synchronized (this.overflowMap) {
      if (this.lastOverflowWrite != null) {
        if (this.lastOverflowWrite.modify(dr, entry, value, async)) {
          return;
        }
      }
      // Create a new one and put it on the front of the list.
      OverflowOplog oo = createOverflowOplog(value.getLength());
      addOverflow(oo);
      this.lastOverflowWrite = oo;
      boolean didIt = oo.modify(dr, entry, value, async);
      assert didIt;
    }
  }

  private long getMaxOplogSizeInBytes() {
    return parent.getMaxOplogSizeInBytes();
  }

  private DirectoryHolder[] getDirectories() {
    return parent.directories;
  }


  /**
   * @param minSize the minimum size this oplog can be
   */
  private OverflowOplog createOverflowOplog(long minSize) {
    lastOverflowDir++;
    if (lastOverflowDir >= getDirectories().length) {
      lastOverflowDir = 0;
    }
    int idx = -1;
    long maxOplogSizeParam = getMaxOplogSizeInBytes();
    if (maxOplogSizeParam < minSize) {
      maxOplogSizeParam = minSize;
    }

    // first look for a directory that has room for maxOplogSize
    for (int i = lastOverflowDir; i < getDirectories().length; i++) {
      long availableSpace = getDirectories()[i].getAvailableSpace();
      if (availableSpace >= maxOplogSizeParam) {
        idx = i;
        break;
      }
    }
    if (idx == -1 && lastOverflowDir != 0) {
      for (int i = 0; i < lastOverflowDir; i++) {
        long availableSpace = getDirectories()[i].getAvailableSpace();
        if (availableSpace >= maxOplogSizeParam) {
          idx = i;
          break;
        }
      }
    }

    if (idx == -1) {
      // if we couldn't find one big enough for the max look for one
      // that has min room
      for (int i = lastOverflowDir; i < getDirectories().length; i++) {
        long availableSpace = getDirectories()[i].getAvailableSpace();
        if (availableSpace >= minSize) {
          idx = i;
          break;
        }
      }
      if (idx == -1 && lastOverflowDir != 0) {
        for (int i = 0; i < lastOverflowDir; i++) {
          long availableSpace = getDirectories()[i].getAvailableSpace();
          if (availableSpace >= minSize) {
            idx = i;
            break;
          }
        }
      }
    }

    if (idx == -1) {
      if (parent.isCompactionEnabled()) { // fix for bug 41835
        idx = lastOverflowDir;
        if (getDirectories()[idx].getAvailableSpace() < minSize) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.DiskRegion_COMPLEXDISKREGIONGETNEXTDIR_MAX_DIRECTORY_SIZE_WILL_GET_VIOLATED__GOING_AHEAD_WITH_THE_SWITCHING_OF_OPLOG_ANY_WAYS_CURRENTLY_AVAILABLE_SPACE_IN_THE_DIRECTORY_IS__0__THE_CAPACITY_OF_DIRECTORY_IS___1,
              new Object[] {Long.valueOf(getDirectories()[idx].getUsedSpace()),
                  Long.valueOf(getDirectories()[idx].getCapacity())}));
        }
      } else {
        throw new DiskAccessException(
            LocalizedStrings.Oplog_DIRECTORIES_ARE_FULL_NOT_ABLE_TO_ACCOMODATE_THIS_OPERATIONSWITCHING_PROBLEM_FOR_ENTRY_HAVING_DISKID_0
                .toLocalizedString("needed " + minSize + " bytes"),
            parent);
      }
    }
    int id = this.overflowOplogId.incrementAndGet();
    lastOverflowDir = idx;
    return new OverflowOplog(id, this, getDirectories()[idx], minSize);
  }

  void addOverflow(OverflowOplog oo) {
    this.overflowMap.put(oo.getOplogId(), oo);
  }

  void removeOverflow(OverflowOplog oo) {
    if (!basicRemoveOverflow(oo)) {
      synchronized (this.compactableOverflowMap) {
        this.compactableOverflowMap.remove(oo.getOplogId());
      }
    }
  }

  boolean basicRemoveOverflow(OverflowOplog oo) {
    if (this.lastOverflowWrite == oo) {
      this.lastOverflowWrite = null;
    }
    return this.overflowMap.remove(oo.getOplogId(), oo);
  }



  public void closeOverflow() {
    for (OverflowOplog oo : this.overflowMap.values()) {
      oo.destroy();
    }
    synchronized (this.compactableOverflowMap) {
      for (OverflowOplog oo : this.compactableOverflowMap.values()) {
        oo.destroy();
      }
    }
  }

  private void removeOverflow(DiskRegion dr, DiskEntry entry) {
    // find the overflow oplog that it is currently in and remove the entry from it
    DiskId id = entry.getDiskId();
    synchronized (id) {
      long oplogId = id.setOplogId(-1);
      if (oplogId != -1) {
        synchronized (this.overflowMap) { // to prevent concurrent remove see bug 41646
          OverflowOplog oplog = getChild((int) oplogId);
          if (oplog != null) {
            oplog.remove(dr, entry);
          }
        }
      }
    }
  }



  void copyForwardForOverflowCompact(DiskEntry de, byte[] valueBytes, int length, byte userBits) {
    synchronized (this.overflowMap) {
      if (this.lastOverflowWrite != null) {
        if (this.lastOverflowWrite.copyForwardForOverflowCompact(de, valueBytes, length,
            userBits)) {
          return;
        }
      }
      OverflowOplog oo = createOverflowOplog(length);
      this.lastOverflowWrite = oo;
      addOverflow(oo);
      boolean didIt = oo.copyForwardForOverflowCompact(de, valueBytes, length, userBits);
      assert didIt;
    }
  }

  public OverflowOplog getChild(long oplogId) {
    // the oplog id is cast to an integer because the overflow
    // map uses integer oplog ids.
    return getChild((int) oplogId);
  }

  public OverflowOplog getChild(int oplogId) {
    OverflowOplog result = this.overflowMap.get(oplogId);
    if (result == null) {
      synchronized (this.compactableOverflowMap) {
        result = this.compactableOverflowMap.get(oplogId);
      }
    }
    return result;
  }


  @Override
  public void create(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    modify(region, entry, value, async);
  }


  @Override
  public void remove(LocalRegion region, DiskEntry entry, boolean async, boolean isClear) {
    removeOverflow(region.getDiskRegion(), entry);
  }

  void addOverflowToBeCompacted(OverflowOplog oplog) {
    synchronized (this.compactableOverflowMap) {
      this.compactableOverflowMap.put(oplog.getOplogId(), oplog);
    }
    basicRemoveOverflow(oplog);
    parent.scheduleCompaction();
  }


  public void getCompactableOplogs(List<CompactableOplog> l, int max) {
    synchronized (this.compactableOverflowMap) {
      Iterator<OverflowOplog> itr = this.compactableOverflowMap.values().iterator();
      while (itr.hasNext() && l.size() < max) {
        OverflowOplog oplog = itr.next();
        if (oplog.needsCompaction()) {
          l.add(oplog);
        }
      }
    }
  }

  void testHookCloseAllOverflowChannels() {
    synchronized (this.overflowMap) {
      for (OverflowOplog oo : this.overflowMap.values()) {
        FileChannel oplogFileChannel = oo.getFileChannel();
        try {
          oplogFileChannel.close();
        } catch (IOException ignore) {
        }
      }
    }
    synchronized (this.compactableOverflowMap) {
      for (OverflowOplog oo : this.compactableOverflowMap.values()) {
        FileChannel oplogFileChannel = oo.getFileChannel();
        try {
          oplogFileChannel.close();
        } catch (IOException ignore) {
        }
      }
    }
  }

  ArrayList<OverflowOplog> testHookGetAllOverflowOplogs() {
    ArrayList<OverflowOplog> result = new ArrayList<OverflowOplog>();
    synchronized (this.overflowMap) {
      for (OverflowOplog oo : this.overflowMap.values()) {
        result.add(oo);
      }
    }
    synchronized (this.compactableOverflowMap) {
      for (OverflowOplog oo : this.compactableOverflowMap.values()) {
        result.add(oo);
      }
    }

    return result;
  }

  void testHookCloseAllOverflowOplogs() {
    synchronized (this.overflowMap) {
      for (OverflowOplog oo : this.overflowMap.values()) {
        oo.close();
      }
    }
    synchronized (this.compactableOverflowMap) {
      for (OverflowOplog oo : this.compactableOverflowMap.values()) {
        oo.close();
      }
    }
  }


  public DiskStoreImpl getParent() {
    return parent;
  }
}
