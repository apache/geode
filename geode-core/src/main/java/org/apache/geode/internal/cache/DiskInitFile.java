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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.InternalInstantiator.InstantiatorAttributesHolder;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.persistence.CanonicalIdHolder;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.persistence.DiskInitFileInterpreter;
import org.apache.geode.internal.cache.persistence.DiskInitFileParser;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PRPersistentConfig;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.versions.DiskRegionVersionVector;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * Does all the IF file work for a DiskStoreImpl.
 *
 *
 * @since GemFire prPersistSprint1
 */
public class DiskInitFile implements DiskInitFileInterpreter {
  private static final Logger logger = LogService.getLogger();

  public static final String IF_FILE_EXT = ".if";

  public static final byte IF_EOF_ID = 0;
  public static final byte END_OF_RECORD_ID = 21;
  static final int OPLOG_FILE_ID_REC_SIZE = 1 + 8 + 1;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Written to IF Byte Format: 8: leastSigBits of UUID 8: mostSigBits of UUID 1: EndOfRecordMarker
   */
  public static final byte IFREC_DISKSTORE_ID = 56;

  /**
   * Written to IF Byte Format: 4: instantiatorID 4: classNameLength classNameLength: className
   * bytes 4: instClassNameLength instClassNameLength: instClassName bytes 1: EndOfRecordMarker
   */
  public static final byte IFREC_INSTANTIATOR_ID = 57;

  /**
   * Written to IF Byte Format: 4: classNameLength classNameLength: className bytes 1:
   * EndOfRecordMarker
   */
  public static final byte IFREC_DATA_SERIALIZER_ID = 58;
  /**
   * Written to IF Used to say that persistent member id is online. Byte Format: RegionId 4:
   * blobLength blobLength: member bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_ONLINE_MEMBER_ID = 59;
  /**
   * Written to IF Used to say that persistent member id is offline. Byte Format: RegionId 4:
   * blobLength blobLength: member bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_OFFLINE_MEMBER_ID = 60;
  /**
   * Written to IF Used to say that persistent member id no longer exists. Byte Format: RegionId 4:
   * blobLength blobLength: member bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_RM_MEMBER_ID = 61;
  /**
   * Written to IF. Used to record the persistent member id of this file. Byte Format: RegionId 4:
   * blobLength blobLength: member bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_MY_MEMBER_INITIALIZING_ID = 62;
  /**
   * Written to IF. Used to record the previous my member id completed initialization. Byte Format:
   * RegionId 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_MY_MEMBER_INITIALIZED_ID = 63;

  /**
   * Written to IF. Used to record create of region Byte Format: RegionId 4: nameLength nameLength:
   * name bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_CREATE_REGION_ID = 64;
  /**
   * Written to IF. Used to record begin of destroy of region Byte Format: RegionId 1:
   * EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_BEGIN_DESTROY_REGION_ID = 65;
  /**
   * Written to IF. Used to record clear of region Byte Format: RegionId 8: oplogEntryId 1:
   * EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_CLEAR_REGION_ID = 66;
  /**
   * Written to IF. Used to record that the end of a destroy region. Byte Format: RegionId 1:
   * EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_END_DESTROY_REGION_ID = 67;
  /**
   * Written to IF. Used to record that a region is about to be partially destroyed Byte Format:
   * RegionId 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID = 68;
  /**
   * Written to IF. Used to record that a region is partially destroyed Byte Format: RegionId 1:
   * EndOfRecordMarker
   *
   * @since GemFire prPersistSprint1
   */
  public static final byte IFREC_END_PARTIAL_DESTROY_REGION_ID = 69;

  /**
   * Records the creation of an oplog crf file Byte Format: 8: oplogId 1: EndOfRecord
   */
  public static final byte IFREC_CRF_CREATE = 70;

  /**
   * Records the creation of an oplog drf file Byte Format: 8: oplogId 1: EndOfRecord
   */
  public static final byte IFREC_DRF_CREATE = 71;

  /**
   * Records the deletion of an oplog crf file Byte Format: 8: oplogId 1: EndOfRecord
   */
  public static final byte IFREC_CRF_DELETE = 72;

  /**
   * Records the deletion of an oplog drf file Byte Format: 8: oplogId 1: EndOfRecord
   */
  public static final byte IFREC_DRF_DELETE = 73;

  /**
   * Written to IF. Used to record regions config Byte Format: RegionId 1: lruAlgorithm 1: lruAction
   * 4: lruLimit (int) // no need to ObjectSize during recovery since all data is in blob form 4:
   * concurrencyLevel (int) 4: initialCapacity (int) 4: loadFactor (float) 1: statisticsEnabled
   * (boolean) 1: isBucket (boolean) 1: EndOfRecordMarker
   *
   * Used to read the region configuration for 6.5 disk stores.
   *
   */
  public static final byte IFREC_REGION_CONFIG_ID = 74;

  /*
   * Written to IF Used to say that persistent member id is offline and has the same data on disk as
   * this member Byte Format: RegionId 4: blobLength blobLength: member bytes 1: EndOfRecordMarker
   *
   * @since GemFire prPersistSprint3
   */
  public static final byte IFREC_OFFLINE_AND_EQUAL_MEMBER_ID = 75;
  /**
   * Written to IF. Used to record regions config Byte Format: RegionId 1: lruAlgorithm 1: lruAction
   * 4: lruLimit (int) // no need to ObjectSize during recovery since all data is in blob form 4:
   * concurrencyLevel (int) 4: initialCapacity (int) 4: loadFactor (float) 1: statisticsEnabled
   * (boolean) 1: isBucket (boolean) 4: length of partitionName String bytes (int) length:actual
   * bytes 4: startingBucketId(int) 1: EndOfRecordMarker
   */
  public static final byte IFREC_REGION_CONFIG_ID_66 = 76;


  /**
   * Records the creation of an oplog krf file The presence of this record indicates that the krf
   * file is complete. Byte Format: 8: oplogId 1: EndOfRecord
   */
  public static final byte IFREC_KRF_CREATE = 77;

  /**
   * Records the creation of a persistent partitioned region configuration. Byte Format: variable:
   * pr name 4: total num buckets variable: colocated with 1: EndOfRecord
   */
  public static final byte IFREC_PR_CREATE = 78;

  /**
   * Records the deletion of persistent partitioned region. Byte Format: variable: pr name 1:
   * EndOfRecord
   */
  public static final byte IFREC_PR_DESTROY = 79;


  /**
   * Maps a member id (either a disk store ID or a distributed system id plus a byte) to a single
   * integer, which can be used in oplogs.
   *
   * Byte Format: 4: the number assigned to this id. variable: serialized object representing the
   * ID. variable: pr name 1: EndOfRecord
   */
  public static final byte IFREC_ADD_CANONICAL_MEMBER_ID = 80;
  /**
   * Written to IF Used to say that a disk store has been revoked Byte Format: variable: a
   * PersistentMemberPattern
   *
   * @since GemFire 7.0
   */
  public static final byte IFREC_REVOKE_DISK_STORE_ID = 81;

  /**
   * Written gemfire version to IF Byte Format: 1: version byte from Version.GFE_CURRENT.ordinal 1:
   * EndOfRecord
   *
   * @since GemFire 7.0
   */
  public static final byte IFREC_GEMFIRE_VERSION = 82;

  /**
   * Written to IF. Used to record clear of using an RVV Byte Format: RegionId variable: serialized
   * RVV 1: EndOfRecordMarker
   *
   * @since GemFire 7.0
   */
  public static final byte IFREC_CLEAR_REGION_WITH_RVV_ID = 83;

  /**
   * Written to IF. Used to record regions config Byte Format: RegionId 1: lruAlgorithm 1: lruAction
   * 4: lruLimit (int) // no need to ObjectSize during recovery since all data is in blob form 4:
   * concurrencyLevel (int) 4: initialCapacity (int) 4: loadFactor (float) 1: statisticsEnabled
   * (boolean) 1: isBucket (boolean) variable: partitionName (utf) 4: startingBucketId (int)
   * variable: compressorClassName (utf) 1: versioned (boolean) 1: EndOfRecordMarker
   *
   */
  public static final byte IFREC_REGION_CONFIG_ID_80 = 88;

  /**
   * Persist oplog file magic number. Written once at the beginning of every oplog file; CRF, DRF,
   * KRF, and IF. Followed by 6 byte magic number. Each oplog type has a different magic number
   * Followed by EndOfRecord Fix for bug 43824
   *
   * @since GemFire 8.0
   */
  public static final byte OPLOG_MAGIC_SEQ_ID = 89;

  /**
   * Written to IF. Used to record regions config Byte Format: RegionId 1: lruAlgorithm 1: lruAction
   * 4: lruLimit (int) // no need to ObjectSize during recovery since all data is in blob form 4:
   * concurrencyLevel (int) 4: initialCapacity (int) 4: loadFactor (float) 1: statisticsEnabled
   * (boolean) 1: isBucket (boolean) variable: partitionName (utf) 4: startingBucketId (int)
   * variable: compressorClassName (utf) 1: versioned (boolean) 1: offHeap (boolean) added in 9.0 1:
   * EndOfRecordMarker
   *
   * @since Geode 1.0
   */
  public static final byte IFREC_REGION_CONFIG_ID_90 = 90;

  private final DiskStoreImpl parent;

  private final File ifFile;
  private RandomAccessFile ifRAF;
  private boolean closed;
  // contains the ids of dataSerializers already written to IF
  private final IntOpenHashSet dsIds;
  // contains the ids of instantiators already written to IF
  private final IntOpenHashSet instIds;

  private final LongOpenHashSet crfIds;
  private final LongOpenHashSet drfIds;
  // krfIds uses a concurrent impl because backup
  // can call hasKrf concurrently with cmnKrfCreate
  private final ConcurrentHashSet<Long> krfIds;

  /**
   * Map used to keep track of regions we know of from the DiskInitFile but that do not yet exist
   * (they have not yet been recovered or they have been closed).
   */
  private final Map<Long, PlaceHolderDiskRegion> drMap = new HashMap<Long, PlaceHolderDiskRegion>();
  private final Map<String, PlaceHolderDiskRegion> drMapByName =
      new HashMap<String, PlaceHolderDiskRegion>();

  /**
   * Map of persistent partitioned regions configurations that are stored in this init file.
   */
  private final Map<String, PRPersistentConfig> prMap = new HashMap<String, PRPersistentConfig>();

  private final InternalDataSerializer.RegistrationListener regListener;

  private int ifLiveRecordCount = 0;
  private int ifTotalRecordCount = 0;
  private boolean compactInProgress;
  // the recovered version
  private Version gfversion;


  /**
   * Used to calculate the highest oplog entry id we have seen in a clear entry.
   */
  private long clearOplogEntryIdHWM = DiskStoreImpl.INVALID_ID;

  /**
   * Container for canonical ids held in the disk store. Member ids are canonicalized so they can be
   * written as an integer in the oplogs.
   */
  private final CanonicalIdHolder canonicalIdHolder = new CanonicalIdHolder();

  /**
   * Set of members that have been revoked. We keep track of the revoked members so that we can
   * indicate to the user a member has been revoked, rather is simply conflicting
   */
  private final Set<PersistentMemberPattern> revokedMembers =
      new HashSet<PersistentMemberPattern>();

  private transient long nextSeekPosition;

  private transient boolean gotEOF;

  private void lock(boolean useBackupLock) {
    if (useBackupLock) {
      getDiskStore().getBackupLock().lock();
    }
    this.lock.lock();
  }

  private void unlock(boolean useBackupLock) {
    if (useBackupLock) {
      getDiskStore().getBackupLock().unlock();
    }
    this.lock.unlock();
  }

  private void recoverFromFailedCompaction() {
    File tmpFile = getTempFile();
    if (tmpFile.exists()) {
      // if the temp init file exists then we must have crashed during a compaction.
      // In this case we need to destroy the non temp file and rename the temp file.
      if (this.ifFile.exists()) {
        if (!this.ifFile.delete()) {
          throw new IllegalStateException("Could not delete " + this.ifFile);
        }
        if (!tmpFile.renameTo(this.ifFile)) {
          throw new IllegalStateException("Could not rename " + tmpFile + " to " + this.ifFile);
        }
      }
    }
  }

  private DiskStoreImpl getDiskStore() {
    return this.parent;
  }

  public Version currentRecoveredGFVersion() {
    return this.gfversion;
  }

  DiskStoreID recover() {
    recoverFromFailedCompaction();
    if (!this.ifFile.exists()) {
      // nothing to recover
      // Instead of calling randomUUID which uses SecureRandom which can be slow
      // return UUID.randomUUID();
      // create a UUID using the cheaper Random class.
      return new DiskStoreID(UUID.randomUUID());
    }
    DiskStoreID result = null;
    try {
      FileInputStream fis = null;
      CountingDataInputStream dis = null;
      try {
        fis = new FileInputStream(this.ifFile);
        dis = new CountingDataInputStream(new BufferedInputStream(fis, 8 * 1024),
            this.ifFile.length());
        DiskInitFileParser parser = new DiskInitFileParser(dis, this);
        result = parser.parse();

        this.gotEOF = parser.gotEOF();
        this.nextSeekPosition = dis.getCount();
        if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
          logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "liveRecordCount={} totalRecordCount={}",
              this.ifLiveRecordCount, this.ifTotalRecordCount);
        }
      } finally {
        if (dis != null) {
          dis.close();
        }
        if (fis != null) {
          fis.close();
        }
      }
      for (PlaceHolderDiskRegion drv : this.drMap.values()) {
        if (drv.getMyPersistentID() != null || drv.getMyInitializingID() != null) {
          // Prepare each region we found in the init file for early recovery.
          if (drv.isBucket() || !getDiskStore().getOwnedByRegion()) {
            getDiskStore().getStats().incUncreatedRecoveredRegions(1);
            drv.setRecoveredEntryMap(RegionMapFactory.createVM(drv, getDiskStore(),
                getDiskStore().getInternalRegionArguments()));
            if (!getDiskStore().isOffline()) {
              // schedule it for recovery since we want to recovery region data early now
              getDiskStore().scheduleForRecovery(drv);
            }
            // else if we are validating or offlineCompacting
            // then the scheduleForRecovery is called later in DiskStoreImpl
            // this helps fix bug 42043
          }
        }
      }
    } catch (EOFException ex) {
      // ignore since a partial record write can be caused by a crash
      // throw new
      // DiskAccessException(String.format("Failed to read file during recovery from %s",
      // this.ifFile.getPath()), ex, this.parent);
    } catch (ClassNotFoundException ex) {
      throw new DiskAccessException(
          String.format("Failed to read file during recovery from %s",
              this.ifFile.getPath()),
          ex, this.parent);
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Failed to read file during recovery from %s",
              this.ifFile.getPath()),
          ex, this.parent);
    } catch (CancelException ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug("Oplog::readOplog:Error in recovery as Cache was closed", ignore);
      }
    } catch (RegionDestroyedException ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug("Oplog::readOplog:Error in recovery as Region was destroyed", ignore);
      }
    } catch (IllegalStateException ex) {
      if (!this.parent.isClosing()) {
        throw ex;
      }
    }
    return result;
  }

  @Override
  public void cmnClearRegion(long drId, long clearOplogEntryId) {
    DiskRegionView drv = getDiskRegionById(drId);
    if (drv.getClearOplogEntryId() == DiskStoreImpl.INVALID_ID) {
      this.ifLiveRecordCount++;
    }
    // otherwise previous clear is cancelled so don't change liveRecordCount
    this.ifTotalRecordCount++;
    drv.setClearOplogEntryId(clearOplogEntryId);
    if (clearOplogEntryId > clearOplogEntryIdHWM) {
      clearOplogEntryIdHWM = clearOplogEntryId;
    }
  }

  @Override
  public void cmnClearRegion(long drId,
      ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion) {
    DiskRegionView drv = getDiskRegionById(drId);
    if (drv.getClearRVV() == null) {
      this.ifLiveRecordCount++;
    }
    // otherwise previous clear is cancelled so don't change liveRecordCount
    this.ifTotalRecordCount++;

    DiskStoreID ownerId = parent.getDiskStoreID();
    // Create a fake RVV for clear purposes. We only need to memberToVersion information
    RegionVersionHolder<DiskStoreID> ownerExceptions = memberToVersion.remove(ownerId);
    long ownerVersion = ownerExceptions == null ? 0 : ownerExceptions.getVersion();
    RegionVersionVector rvv = new DiskRegionVersionVector(ownerId, memberToVersion, ownerVersion,
        new ConcurrentHashMap(), 0L, false, ownerExceptions);
    drv.setClearRVV(rvv);
  }

  private int liveRegions = 0; // added for bug 41618

  public boolean hasLiveRegions() {
    lock(false);
    try {
      return this.liveRegions > 0;
    } finally {
      unlock(false);
    }
  }

  @Override
  public void cmnCreateRegion(long drId, String regName) {
    recoverDiskRegion(drId, regName);
    this.liveRegions++;
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnRegionConfig(long drId, byte lruAlgorithm, byte lruAction, int lruLimit,
      int concurrencyLevel, int initialCapacity, float loadFactor, boolean statisticsEnabled,
      boolean isBucket, EnumSet<DiskRegionFlag> flags, String partitionName, int startingBucketId,
      String compressorClassName, boolean offHeap) {
    DiskRegionView dr = getDiskRegionById(drId);
    if (dr != null) {
      // We need to add the IS_WITH_VERSIONING to persistent regions
      // during the upgrade. Previously, all regions had versioning enabled
      // but now only regions that have this flag will have versioning enabled.
      //
      // We don't want gateway queues to turn on versioning. Unfortunately, the only
      // way to indentify that a region is a gateway queue is by the region
      // name.
      if (Version.GFE_80.compareTo(currentRecoveredGFVersion()) > 0
          && !dr.getName().contains("_SERIAL_GATEWAY_SENDER_QUEUE")
          && !dr.getName().contains("_PARALLEL__GATEWAY__SENDER__QUEUE")) {
        flags.add(DiskRegionFlag.IS_WITH_VERSIONING);
      }
      dr.setConfig(lruAlgorithm, lruAction, lruLimit, concurrencyLevel, initialCapacity, loadFactor,
          statisticsEnabled, isBucket, flags, partitionName, startingBucketId, compressorClassName,
          offHeap);

      // Just count this as a live record even though it is possible
      // that we have an extra one due to the config changing while
      // we were offline.
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  @Override
  public boolean cmnPRCreate(String name, PRPersistentConfig config) {
    if (this.prMap.put(name, config) == null) {
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
      this.liveRegions++;
      return true;
    }

    return false;
  }

  @Override
  public void cmnGemfireVersion(Version version) {
    this.gfversion = version;
  }

  @Override
  public boolean cmnPRDestroy(String name) {
    if (this.prMap.remove(name) != null) {
      this.ifLiveRecordCount--;
      this.ifTotalRecordCount++;
      this.liveRegions--;
      return true;
    }
    return false;
  }

  @Override
  public void cmnAddCanonicalMemberId(int id, Object object) {
    this.canonicalIdHolder.addMapping(id, object);
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnRmMemberId(long drId, PersistentMemberID pmid) {
    DiskRegionView dr = getDiskRegionById(drId);
    if (dr != null) {
      if (!dr.rmOnlineMember(pmid)) {
        if (!dr.rmOfflineMember(pmid)) {
          dr.rmEqualMember(pmid);
        }
      }
      // since we removed a member don't inc the live count
      // In fact decrement it by one since both this record
      // and the previous one are both garbage.
      this.ifLiveRecordCount--;
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  @Override
  public void cmnOfflineMemberId(long drId, PersistentMemberID pmid) {
    DiskRegionView dr = getDiskRegionById(drId);
    if (dr != null) {
      dr.addOfflineMember(pmid);
      if (dr.rmOnlineMember(pmid) || dr.rmEqualMember(pmid)) {
        this.ifLiveRecordCount--;
      }
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  @Override
  public void cmdOfflineAndEqualMemberId(long drId, PersistentMemberID pmid) {
    DiskRegionView dr = getDiskRegionById(drId);
    if (dr != null) {
      if (this.parent.upgradeVersionOnly
          && Version.GFE_70.compareTo(currentRecoveredGFVersion()) > 0) {
        dr.addOnlineMember(pmid);
        if (dr.rmOfflineMember(pmid)) {
          this.ifLiveRecordCount--;
        }
      } else {
        dr.addOfflineAndEqualMember(pmid);
        if (dr.rmOnlineMember(pmid) || dr.rmOfflineMember(pmid)) {
          this.ifLiveRecordCount--;
        }
      }
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  @Override
  public void cmnOnlineMemberId(long drId, PersistentMemberID pmid) {
    DiskRegionView dr = getDiskRegionById(drId);
    if (dr != null) {
      dr.addOnlineMember(pmid);
      if (dr.rmOfflineMember(pmid) || dr.rmEqualMember(pmid)) {
        this.ifLiveRecordCount--;
      }
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  @Override
  public void cmnDataSerializerId(Class dsc) {
    if (dsc != null) {
      DataSerializer ds = InternalDataSerializer.register(dsc, /* dsId, */ true);
      this.dsIds.add(ds.getId());
    }
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnInstantiatorId(int id, Class c, Class ic) {
    if (c != null && ic != null) {
      InternalInstantiator.register(c, ic, id, true);
      this.instIds.add(id);
    }
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnInstantiatorId(int id, String cn, String icn) {
    if (cn != null && icn != null) {
      InternalInstantiator.register(cn, icn, id, true);
      this.instIds.add(id);
    }
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnCrfCreate(long oplogId) {
    this.crfIds.add(oplogId);
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnDrfCreate(long oplogId) {
    this.drfIds.add(oplogId);
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public void cmnKrfCreate(long oplogId) {
    this.krfIds.add(oplogId);
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;
  }

  @Override
  public boolean cmnCrfDelete(long oplogId) {
    if (this.krfIds.remove(oplogId)) {
      this.ifLiveRecordCount--;
      this.ifTotalRecordCount++;
    }
    if (this.crfIds.remove(oplogId)) {
      this.ifLiveRecordCount--;
      this.ifTotalRecordCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean cmnDrfDelete(long oplogId) {
    if (this.drfIds.remove(oplogId)) {
      this.ifLiveRecordCount--;
      this.ifTotalRecordCount++;
      return true;
    } else {
      return false;
    }
  }

  public boolean isCRFOplogIdPresent(long crfId) {
    return this.crfIds.contains(crfId);
  }

  public boolean isDRFOplogIdPresent(long drfId) {
    return this.drfIds.contains(drfId);
  }

  public void verifyOplogs(LongOpenHashSet foundCrfs, LongOpenHashSet foundDrfs) {
    verifyOplogs(foundCrfs, foundDrfs, this.crfIds, this.drfIds);
  }

  public void verifyOplogs(LongOpenHashSet foundCrfs, LongOpenHashSet foundDrfs,
      LongOpenHashSet expectedCrfIds, LongOpenHashSet expectedDrfIds) {
    LongOpenHashSet missingCrfs = calcMissing(foundCrfs, expectedCrfIds);
    LongOpenHashSet missingDrfs = calcMissing(foundDrfs, expectedDrfIds);
    // Note that finding extra ones is ok; it is possible we died just
    // after creating one but before we could record it in the if file
    // Or died just after deleting it but before we could record it in the if file.
    boolean failed = false;
    String msg = null;
    if (!missingCrfs.isEmpty()) {
      failed = true;
      msg = "*.crf files with these ids: " + Arrays.toString(missingCrfs.toArray());
    }
    if (!missingDrfs.isEmpty()) {
      failed = true;
      if (msg == null) {
        msg = "";
      } else {
        msg += ", ";
      }
      msg += "*.drf files with these ids: " + Arrays.toString(missingDrfs.toArray());
    }
    if (failed) {
      msg = "The following required files could not be found: " + msg + ".";
      throw new IllegalStateException(msg);
    }
  }

  private LongOpenHashSet calcMissing(LongOpenHashSet found, LongOpenHashSet expected) {
    LongOpenHashSet missing = new LongOpenHashSet(expected);
    missing.removeAll(found);
    return missing;
  }

  boolean hasKrf(long oplogId) {
    return krfIds.contains(oplogId);
  }

  DiskRegionView takeDiskRegionByName(String name) {
    lock(false);
    try {
      DiskRegionView result = this.drMapByName.remove(name);
      if (result != null) {
        this.drMap.remove(result.getId());
      }
      return result;
    } finally {
      unlock(false);
    }
  }

  Map<Long, PlaceHolderDiskRegion> getDRMap() {
    lock(false);
    try {
      return new HashMap<Long, PlaceHolderDiskRegion>(drMap);
    } finally {
      unlock(false);
    }
  }

  DiskRegion createDiskRegion(DiskStoreImpl dsi, String name, boolean isBucket,
      boolean isPersistBackup, boolean overflowEnabled, boolean isSynchronous,
      DiskRegionStats stats, CancelCriterion cancel, DiskExceptionHandler exceptionHandler,
      RegionAttributes ra, EnumSet<DiskRegionFlag> flags, String partitionName,
      int startingBucketId, Compressor compressor, boolean offHeap) {
    lock(true);
    try {
      // need to call the constructor and addDiskRegion while synced
      DiskRegion result = new DiskRegion(dsi, name, isBucket, isPersistBackup, overflowEnabled,
          isSynchronous, stats, cancel, exceptionHandler, ra, flags, partitionName,
          startingBucketId, compressor == null ? null : compressor.getClass().getName(), offHeap);
      dsi.addDiskRegion(result);
      return result;
    } finally {
      unlock(true);
    }
  }

  DiskRegionView getDiskRegionByName(String name) {
    lock(false);
    try {
      return this.drMapByName.get(name);
    } finally {
      unlock(false);
    }
  }

  DiskRegionView getDiskRegionByPrName(String name) {
    lock(false);
    try {
      for (PlaceHolderDiskRegion dr : this.drMapByName.values()) {
        if (dr.isBucket()) {
          if (name.equals(dr.getPrName())) {
            return dr;
          }
        }
      }
      return null;
    } finally {
      unlock(false);
    }
  }

  private DiskRegionView getDiskRegionById(long drId) {
    DiskRegionView result = this.drMap.get(drId);
    if (result == null) {
      result = this.parent.getById(drId);
    }
    return result;
  }


  private void recoverDiskRegion(long drId, String regName) {
    // Whatever the last create region drId we see we remember
    // in the DiskStore. Note that this could be a region that is destroyed
    // (we will not know until we see a later destroy region record)
    // but since drId's can wrap around into negative numbers whatever
    // the last one we see is the right one to remember.
    this.parent.recoverRegionId(drId);
    PlaceHolderDiskRegion dr = new PlaceHolderDiskRegion(this.parent, drId, regName);
    Object old = this.drMap.put(drId, dr);
    assert old == null;
    PlaceHolderDiskRegion oldDr = this.drMapByName.put(regName, dr);
    if (oldDr != null) {
      this.drMap.remove(oldDr.getId()); // fix for bug 42043
    }
    // don't schedule for recovery until we know it was not destroyed
  }

  /**
   * Maximum number of bytes used to encode a DiskRegion id.
   */
  static final int DR_ID_MAX_BYTES = 9;

  private void writeIFRecord(byte b, DiskRegionView dr) {
    assert lock.isHeldByCurrentThread();
    try {
      ByteBuffer bb = getIFWriteBuffer(1 + DR_ID_MAX_BYTES + 1);
      bb.put(b);
      putDiskRegionID(bb, dr.getId());
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeIFRecord(byte b, DiskRegionView dr, long v) {
    assert lock.isHeldByCurrentThread();
    try {
      ByteBuffer bb = getIFWriteBuffer(1 + DR_ID_MAX_BYTES + 8 + 1);
      bb.put(b);
      putDiskRegionID(bb, dr.getId());
      bb.putLong(v);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeIFRecord(byte b, long v) {
    assert lock.isHeldByCurrentThread();
    try {
      ByteBuffer bb = getIFWriteBuffer(OPLOG_FILE_ID_REC_SIZE);
      bb.put(b);
      bb.putLong(v);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeIFRecord(byte b, DiskRegionView dr, String s) {
    assert lock.isHeldByCurrentThread();
    try {
      int hdosSize = 1 + DR_ID_MAX_BYTES + estimateByteSize(s) + 1;
      if (hdosSize < 32) {
        hdosSize = 32;
      }
      HeapDataOutputStream hdos = new HeapDataOutputStream(hdosSize, Version.CURRENT);
      hdos.write(b);
      writeDiskRegionID(hdos, dr.getId());
      hdos.writeUTF(s);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, true);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeIFRecord(byte b, long regionId, String fileName, Object compactorInfo) {
    assert lock.isHeldByCurrentThread();
    try {
      int hdosSize = 1 + DR_ID_MAX_BYTES + estimateByteSize(fileName) + 1;
      if (hdosSize < 32) {
        hdosSize = 32;
      }
      HeapDataOutputStream hdos = new HeapDataOutputStream(hdosSize, Version.CURRENT);
      hdos.write(b);
      writeDiskRegionID(hdos, regionId);
      hdos.writeUTF(fileName);
      // TODO - plum the correct compactor info to this point, to optimize
      // serialization
      DataSerializer.writeObject(compactorInfo, hdos);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, true);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeIFRecord(byte b, long regionId, String fileName) {
    assert lock.isHeldByCurrentThread();
    try {
      int hdosSize = 1 + DR_ID_MAX_BYTES + estimateByteSize(fileName) + 1;
      if (hdosSize < 32) {
        hdosSize = 32;
      }
      HeapDataOutputStream hdos = new HeapDataOutputStream(hdosSize, Version.CURRENT);
      hdos.write(b);
      writeDiskRegionID(hdos, regionId);
      hdos.writeUTF(fileName);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, true);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private int estimateByteSize(String s) {
    return s == null ? 0 : ((s.length() + 1) * 3);
  }

  private void writePMIDRecord(byte opcode, DiskRegionView dr, PersistentMemberID pmid,
      boolean doStats) {
    assert lock.isHeldByCurrentThread();
    try {
      byte[] pmidBytes = pmidToBytes(pmid);
      ByteBuffer bb = getIFWriteBuffer(1 + DR_ID_MAX_BYTES + 4 + pmidBytes.length + 1);
      bb.put(opcode);
      putDiskRegionID(bb, dr.getId());
      bb.putInt(pmidBytes.length);
      bb.put(pmidBytes);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, doStats);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void putDiskRegionID(ByteBuffer bb, long drId) {
    // If the drId is <= 255 (max unsigned byte) then
    // encode it as a single byte.
    // Otherwise write a byte whose value is the number of bytes
    // it will be encoded by and then follow it with that many bytes.
    // Note that drId are not allowed to have a value in the range 1..8 inclusive.
    if (drId >= 0 && drId <= 255) {
      bb.put((byte) drId);
    } else {
      byte bytesNeeded = (byte) Oplog.bytesNeeded(drId);
      bb.put(bytesNeeded);
      byte[] bytes = new byte[bytesNeeded];
      for (int i = bytesNeeded - 1; i >= 0; i--) {
        bytes[i] = (byte) (drId & 0xFF);
        drId >>= 8;
      }
      bb.put(bytes);
    }
  }

  static void writeDiskRegionID(DataOutput dos, long drId) throws IOException {
    // If the drId is <= 255 (max unsigned byte) then
    // encode it as a single byte.
    // Otherwise write a byte whose value is the number of bytes
    // it will be encoded by and then follow it with that many bytes.
    // Note that drId are not allowed to have a value in the range 1..8 inclusive.
    if (drId >= 0 && drId <= 255) {
      dos.write((byte) drId);
    } else {
      byte bytesNeeded = (byte) Oplog.bytesNeeded(drId);
      dos.write(bytesNeeded);
      byte[] bytes = new byte[bytesNeeded];
      for (int i = bytesNeeded - 1; i >= 0; i--) {
        bytes[i] = (byte) (drId & 0xFF);
        drId >>= 8;
      }
      dos.write(bytes);
    }
  }

  static long readDiskRegionID(DataInput dis) throws IOException {
    int bytesToRead = dis.readUnsignedByte();
    if (bytesToRead <= DiskStoreImpl.MAX_RESERVED_DRID
        && bytesToRead >= DiskStoreImpl.MIN_RESERVED_DRID) {
      long result = dis.readByte(); // we want to sign extend this first byte
      bytesToRead--;
      while (bytesToRead > 0) {
        result <<= 8;
        result |= dis.readUnsignedByte(); // no sign extension
        bytesToRead--;
      }
      return result;
    } else {
      return bytesToRead;
    }
  }

  private void cmnAddMyInitializingPMID(DiskRegionView dr, PersistentMemberID pmid) {
    if (dr != null) {
      if (dr.addMyInitializingPMID(pmid) == null) {
        this.ifLiveRecordCount++;
      }
      this.ifTotalRecordCount++;
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  private void cmnMarkInitialized(DiskRegionView dr) {
    // dec since this initializeId is overriding a previous one
    // It actually doesn't override myInitializing
    // this.ifLiveRecordCount--;
    // don't count this as a record in the totalRecCount
    if (dr != null) {
      dr.markInitialized();
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  private void cmnBeginDestroyRegion(DiskRegionView dr) {
    // don't count it is a small record

    if (dr != null) {
      dr.markBeginDestroyRegion();
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  private void cmnEndDestroyRegion(DiskRegionView dr) {
    // Figure out how may other records this freed up.

    if (dr != null) {
      if (dr.getClearOplogEntryId() != DiskStoreImpl.INVALID_ID) {
        // one for the clear record
        this.ifLiveRecordCount--;
      }
      // one for each online member
      this.ifLiveRecordCount -= dr.getOnlineMembers().size();
      // one for each offline member
      this.ifLiveRecordCount -= dr.getOfflineMembers().size();
      // one for each equal member
      this.ifLiveRecordCount -= dr.getOfflineAndEqualMembers().size();



      // one for the CREATE_REGION
      this.ifLiveRecordCount--;

      // one for the regions memberId
      if (dr.getMyPersistentID() != null) {
        this.ifLiveRecordCount--;
      }

      this.liveRegions--;
      this.drMap.remove(dr.getId());
      this.drMapByName.remove(dr.getName());
      this.parent.rmById(dr.getId());

      dr.markEndDestroyRegion();
    } else {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "bad disk region id!");
      } else {
        throw new IllegalStateException("bad disk region id");
      }
    }
  }

  private void cmnBeginPartialDestroyRegion(DiskRegionView dr) {
    // count the begin as both live and total
    this.ifLiveRecordCount++;
    this.ifTotalRecordCount++;

    dr.markBeginDestroyDataStorage();
  }

  private void cmnEndPartialDestroyRegion(DiskRegionView dr) {
    // no need to count this small record

    // Figure out how may other records this freed up.
    if (dr.getClearOplogEntryId() != DiskStoreImpl.INVALID_ID) {
      // one for the clear record
      this.ifLiveRecordCount--;
    }
    // Figure out how may other records this freed up.
    if (dr.getMyPersistentID() != null) {
      // one for the regions memberId
      this.ifLiveRecordCount--;
    }

    dr.markEndDestroyDataStorage();
  }

  /**
   * Write the specified instantiator to the file.
   */
  private void saveInstantiator(Instantiator inst) {
    saveInstantiator(inst.getId(), inst.getClass().getName(),
        inst.getInstantiatedClass().getName());
  }

  private void saveInstantiator(int id, String instantiatorClassName,
      String instantiatedClassName) {
    lock(true);
    try {
      if (!this.compactInProgress && this.instIds.contains(id)) {
        // instantiator already written to disk so just return
        return;
      }
      final byte[] classNameBytes = classNameToBytes(instantiatorClassName);
      final byte[] instClassNameBytes = classNameToBytes(instantiatedClassName);
      ByteBuffer bb =
          getIFWriteBuffer(1 + 4 + 4 + classNameBytes.length + 4 + instClassNameBytes.length + 1);
      bb.put(IFREC_INSTANTIATOR_ID);
      bb.putInt(id);
      bb.putInt(classNameBytes.length);
      bb.put(classNameBytes);
      bb.putInt(instClassNameBytes.length);
      bb.put(instClassNameBytes);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb);
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Failed saving instantiator to disk because: %s",
              ex),
          this.parent);
    } finally {
      unlock(true);
    }
  }

  private void saveInstantiators() {
    Object[] objects = InternalInstantiator.getInstantiatorsForSerialization();
    for (Object obj : objects) {
      if (obj instanceof Instantiator) {
        saveInstantiator((Instantiator) obj);
      } else {
        InstantiatorAttributesHolder iah = (InstantiatorAttributesHolder) obj;
        saveInstantiator(iah.getId(), iah.getInstantiatorClassName(),
            iah.getInstantiatedClassName());
      }
    }
  }

  /**
   * Returns the bytes used to represent a class in an oplog.
   */
  private static byte[] classToBytes(Class c) {
    return classNameToBytes(c.getName());
  }

  /**
   * Returns the bytes used to represent a class in an oplog.
   */
  private static byte[] classNameToBytes(String cn) {
    return cn.getBytes(); // use default encoder
  }

  /**
   * Write the specified DataSerializer to the file.
   */
  private void saveDataSerializer(DataSerializer ds) {
    lock(true);
    try {
      if (!this.compactInProgress && this.dsIds.contains(ds.getId())) {
        // dataSerializer already written to disk so just return
        return;
      }
      final byte[] classNameBytes = classToBytes(ds.getClass());
      ByteBuffer bb = getIFWriteBuffer(1 + 4 + classNameBytes.length + 1);
      bb.put(IFREC_DATA_SERIALIZER_ID);
      bb.putInt(classNameBytes.length);
      bb.put(classNameBytes);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb);
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Failed saving data serializer to disk because: %s",
              ex),
          this.parent);
    } finally {
      unlock(true);
    }
  }

  private void saveDataSerializers() {
    DataSerializer[] dataSerializers = InternalDataSerializer.getSerializers();
    for (int i = 0; i < dataSerializers.length; i++) {
      saveDataSerializer(dataSerializers[i]);
    }
  }

  private void saveGemfireVersion() {
    if (this.gfversion == null) {
      this.gfversion = Version.CURRENT;
    }
    writeGemfireVersion(this.gfversion);
  }

  private void stopListeningForDataSerializerChanges() {
    if (this.regListener != null) {
      InternalDataSerializer.removeRegistrationListener(this.regListener);
    }
  }

  public long getMaxRecoveredClearEntryId() {
    return clearOplogEntryIdHWM;
  }

  private ByteBuffer getIFWriteBuffer(int size) {
    return ByteBuffer.allocate(size);
  }

  private void writeIFRecord(ByteBuffer bb) throws IOException {
    writeIFRecord(bb, true);
  }

  private void writeIFRecord(ByteBuffer bb, boolean doStats) throws IOException {
    assert lock.isHeldByCurrentThread();
    if (this.closed) {
      throw new DiskAccessException("The disk store is closed", parent);
    }

    this.ifRAF.write(bb.array(), 0, bb.position());
    if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_WRITES_VERBOSE, "DiskInitFile writeIFRecord bb[0] = {}",
          bb.array()[0]);
    }
    if (doStats) {
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
    compactIfNeeded();
  }

  private void writeIFRecord(HeapDataOutputStream hdos, boolean doStats) throws IOException {
    assert lock.isHeldByCurrentThread();
    if (this.closed) {
      throw new DiskAccessException("The disk store is closed", parent);
    }
    hdos.sendTo(this.ifRAF);
    if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_WRITES_VERBOSE, "DiskInitFile writeIFRecord HDOS");
    }
    if (doStats) {
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
    compactIfNeeded();
  }

  /**
   * If the file is smaller than this constant then it does not need to be compacted.
   */
  private static final long MIN_SIZE_BEFORE_COMPACT = 1024 * 1024;
  /**
   * If the ratio of live vs. dead is not less than this constant then no need to compact.
   */
  private static final double COMPACT_RATIO = 0.5;

  private void compactIfNeeded() {
    lock(true);
    try {
      if (this.compactInProgress)
        return;
      if (this.ifTotalRecordCount == 0)
        return;
      if (this.ifTotalRecordCount == this.ifLiveRecordCount)
        return;
      if (this.ifRAF.length() <= MIN_SIZE_BEFORE_COMPACT)
        return;
      if ((double) this.ifLiveRecordCount / (double) this.ifTotalRecordCount > COMPACT_RATIO)
        return;
      compact();
    } catch (IOException ignore) {
      return;
    } finally {
      unlock(true);
    }
  }

  private File getTempFile() {
    return new File(this.ifFile.getAbsolutePath() + "tmp");
  }

  public File getIFFile() {
    return this.ifFile;
  }

  private void compact() {
    lock(true);
    this.compactInProgress = true;
    try {
      try {
        this.ifRAF.close();
      } catch (IOException ignore) {
      }
      // rename the old file to tmpFile
      File tmpFile = getTempFile();
      if (this.ifFile.renameTo(tmpFile)) {
        boolean success = false;
        try {
          // create the new file
          openRAF();
          // fill the new file with data
          writeLiveData();
          success = true;

          // delete the old file
          if (!tmpFile.delete()) {
            throw new DiskAccessException("could not delete temporary file " + tmpFile,
                this.parent);
          }
        } catch (DiskAccessException ignore) {
          if (logger.isDebugEnabled()) {
            logger.debug("Exception compacting init file {}", this, ignore);
          }
        } finally {
          if (!success) {
            // if we failed
            // close the new one and delete it
            try {
              this.ifRAF.close();
            } catch (IOException ignore2) {
            }
            if (!this.ifFile.delete()) {
              throw new DiskAccessException("could not delete file " + this.ifFile, this.parent);
            }
            if (!tmpFile.renameTo(this.ifFile)) {
              throw new DiskAccessException(
                  "could not rename file " + tmpFile + " to " + this.ifFile, this.parent);
            }
            // reopen the old file since we couldn't write the new one
            openRAF();
            // reset the counts to 0 so we will try a compaction again
            // in the future but not right away.
            this.ifLiveRecordCount = 0;
            this.ifTotalRecordCount = 0;
          }
        }
      } else {
        // reopen the old file since we couldn't rename it
        openRAF();
        // reset the counts to 0 so we will try a compaction again
        // in the future but not right away.
        this.ifLiveRecordCount = 0;
        this.ifTotalRecordCount = 0;
      }
    } finally {
      this.compactInProgress = false;
      unlock(true);
    }
  }

  public void copyTo(File targetDir) throws IOException {
    lock(false);
    try {
      FileUtils.copyFileToDirectory(this.ifFile, targetDir);
    } finally {
      unlock(false);
    }
  }

  private void openRAF() {
    if (DiskStoreImpl.PREALLOCATE_IF) {
      openRAF2();
      return;
    }

    try {
      this.ifRAF = new RandomAccessFile(this.ifFile, getFileMode());
      long len = this.ifRAF.length();
      if (len != 0) {
        this.ifRAF.seek(len);
      }
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Could not open %s.", this.ifFile.getPath()), ex,
          this.parent);
    }
  }

  protected String getFileMode() {
    return DiskStoreImpl.SYNC_IF_WRITES ? "rwd" : "rw";
  }

  private void openRAF2() {
    try {
      this.ifRAF = new RandomAccessFile(this.ifFile, getFileMode());
      long len = this.ifRAF.length();
      if (len != 0) {
        // this.ifRAF.seek(len);
        if (this.gotEOF) {
          this.ifRAF.seek(this.nextSeekPosition - 1);
        } else {
          this.ifRAF.seek(this.nextSeekPosition);
        }
      } else {
        // pre-allocate the if file using some percentage of max Oplog size but
        // with max of 10M and min of 1M
        long maxSizeInMB = Math.min(Math.max(this.parent.getMaxOplogSize() / 200L, 1L), 10L);
        byte[] buffer = new byte[(1024 * 1024)];
        for (int i = 0; i < maxSizeInMB; i++) {
          this.ifRAF.write(buffer);
        }
        this.ifRAF.seek(0L);
      }
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Could not open %s.", this.ifFile.getPath()), ex,
          this.parent);
    }
  }

  /**
   * Write all live data to the init file
   */
  private void writeLiveData() {
    lock(true);
    try {
      this.ifLiveRecordCount = 0;
      this.ifTotalRecordCount = 0;
      writeDiskStoreId();
      saveGemfireVersion();
      saveInstantiators();
      saveDataSerializers();
      saveCrfIds();
      saveDrfIds();
      saveKrfIds();
      for (DiskRegionView drv : this.drMap.values()) {
        writeLiveData(drv);
      }
      for (DiskRegionView drv : this.parent.getDiskRegions()) {
        writeLiveData(drv);
      }
      savePRConfigs();
      saveCanonicalIds();
      saveRevokedMembers();
      if (logger.isDebugEnabled()) {
        logger.debug("After compacting init file lrc={} trc={}", this.ifLiveRecordCount,
            this.ifTotalRecordCount);
      }
    } finally {
      unlock(true);
    }
  }

  private void saveCrfIds() {
    for (LongIterator i = this.crfIds.iterator(); i.hasNext();) {
      writeIFRecord(IFREC_CRF_CREATE, i.next());
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
  }

  private void saveDrfIds() {
    for (LongIterator i = this.drfIds.iterator(); i.hasNext();) {
      writeIFRecord(IFREC_DRF_CREATE, i.next());
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
  }

  private void saveKrfIds() {
    for (Iterator<Long> i = this.krfIds.iterator(); i.hasNext();) {
      writeIFRecord(IFREC_KRF_CREATE, i.next());
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
  }

  private void savePRConfigs() {
    for (Map.Entry<String, PRPersistentConfig> entry : prMap.entrySet()) {
      writePRCreate(entry.getKey(), entry.getValue());
      this.ifLiveRecordCount++;
      this.ifTotalRecordCount++;
    }
  }

  private void saveCanonicalIds() {
    Int2ObjectOpenHashMap mappings = canonicalIdHolder.getAllMappings();
    for (ObjectIterator<Int2ObjectMap.Entry<?>> i = mappings.int2ObjectEntrySet().fastIterator(); i
        .hasNext();) {
      Int2ObjectMap.Entry<?> entry = i.next();
      writeCanonicalId(entry.getIntKey(), entry.getValue());
    }
  }

  private void saveRevokedMembers() {
    for (PersistentMemberPattern revoked : revokedMembers) {
      writeRevokedMember(revoked);
    }
  }

  private void writeDiskStoreId() {
    lock(true);
    try {
      ByteBuffer bb = getIFWriteBuffer(1 + 6 + 1);
      bb.put(OPLOG_MAGIC_SEQ_ID);
      bb.put(Oplog.OPLOG_TYPE.IF.getBytes(), 0, Oplog.OPLOG_TYPE.getLen());
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records

      bb = getIFWriteBuffer(1 + 8 + 8 + 1);
      bb.put(IFREC_DISKSTORE_ID);
      bb.putLong(parent.getDiskStoreID().getLeastSignificantBits());
      bb.putLong(parent.getDiskStoreID().getMostSignificantBits());
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    } finally {
      unlock(true);
    }
  }

  private void writeRevokedMember(PersistentMemberPattern revoked) {
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
      hdos.write(IFREC_REVOKE_DISK_STORE_ID);
      InternalDataSerializer.invokeToData(revoked, hdos);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeRegionConfig(DiskRegionView drv) {
    try {
      int len = estimateByteSize(drv.getPartitionName());
      int comprLen = estimateByteSize(drv.getCompressorClassName());
      HeapDataOutputStream bb = new HeapDataOutputStream(
          1 + DR_ID_MAX_BYTES + 1 + 1 + 4 + 4 + 4 + 1 + 1 + 4 + len + 4 + 1 + 1 + 1,
          Version.CURRENT);
      bb.write(IFREC_REGION_CONFIG_ID_90);
      writeDiskRegionID(bb, drv.getId());
      bb.write(drv.getLruAlgorithm());
      bb.write(drv.getLruAction());
      bb.writeInt(drv.getLruLimit());
      bb.writeInt(drv.getConcurrencyLevel());
      bb.writeInt(drv.getInitialCapacity());
      bb.writeFloat(drv.getLoadFactor());
      bb.write((byte) (drv.getStatisticsEnabled() ? 1 : 0));
      bb.write((byte) (drv.isBucket() ? 1 : 0));
      final EnumSet<DiskRegionFlag> flags = drv.getFlags();
      bb.writeUTF(drv.getPartitionName());
      bb.writeInt(drv.getStartingBucketId());
      bb.writeUTF(drv.getCompressorClassName() == null ? "" : drv.getCompressorClassName());
      bb.writeBoolean(flags.contains(DiskRegionFlag.IS_WITH_VERSIONING));
      // TODO the offheap flag needs to be in a new version
      bb.writeBoolean(drv.getOffHeap());
      bb.write(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writePRCreate(String name, PRPersistentConfig config) {
    try {
      int nameLength = estimateByteSize(name);
      String colocatedWith = config.getColocatedWith();
      colocatedWith = colocatedWith == null ? "" : colocatedWith;
      int colocatedLength = estimateByteSize(colocatedWith);
      HeapDataOutputStream hdos =
          new HeapDataOutputStream(1 + nameLength + 4 + colocatedLength + 1, Version.CURRENT);
      hdos.write(IFREC_PR_CREATE);
      hdos.writeUTF(name);
      hdos.writeInt(config.getTotalNumBuckets());
      hdos.writeUTF(colocatedWith);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, false);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writePRDestroy(String name) {
    try {
      int nameLength = estimateByteSize(name);
      HeapDataOutputStream hdos = new HeapDataOutputStream(1 + nameLength + 4 + 1, Version.CURRENT);
      hdos.write(IFREC_PR_DESTROY);
      hdos.writeUTF(name);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, false);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeCanonicalId(int id, Object object) {
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
      hdos.write(IFREC_ADD_CANONICAL_MEMBER_ID);
      hdos.writeInt(id);
      DataSerializer.writeObject(object, hdos);
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, true);
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }
  }

  private void writeLiveData(DiskRegionView drv) {
    writeIFRecord(IFREC_CREATE_REGION_ID, drv, drv.getName());
    writeRegionConfig(drv);
    if (drv.wasAboutToDestroy()) {
      writeIFRecord(IFREC_BEGIN_DESTROY_REGION_ID, drv);
    } else if (drv.wasAboutToDestroyDataStorage()) {
      writeIFRecord(IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID, drv);
    }
    if (drv.getClearOplogEntryId() != DiskStoreImpl.INVALID_ID) {
      writeIFRecord(IFREC_CLEAR_REGION_ID, drv, drv.getClearOplogEntryId());
      this.ifTotalRecordCount++;
      this.ifLiveRecordCount++;
    }
    if (drv.getClearRVV() != null) {
      writeClearRecord(drv, drv.getClearRVV());
    }
    for (PersistentMemberID pmid : drv.getOnlineMembers()) {
      writePMIDRecord(IFREC_ONLINE_MEMBER_ID, drv, pmid, true);
    }
    for (PersistentMemberID pmid : drv.getOfflineMembers()) {
      writePMIDRecord(IFREC_OFFLINE_MEMBER_ID, drv, pmid, true);
    }
    for (PersistentMemberID pmid : drv.getOfflineAndEqualMembers()) {
      writePMIDRecord(IFREC_OFFLINE_AND_EQUAL_MEMBER_ID, drv, pmid, true);
    }
    if (drv.getMyPersistentID() != null) {
      writePMIDRecord(IFREC_MY_MEMBER_INITIALIZING_ID, drv, drv.getMyPersistentID(), true);
      writeIFRecord(IFREC_MY_MEMBER_INITIALIZED_ID, drv);
    }
    if (drv.getMyInitializingID() != null) {
      writePMIDRecord(IFREC_MY_MEMBER_INITIALIZING_ID, drv, drv.getMyInitializingID(), true);
    }
  }

  void forceCompaction() {
    compact();
  }

  private byte[] pmidToBytes(PersistentMemberID id) {
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
      InternalDataSerializer.invokeToData(id, hdos);
      return hdos.toByteArray();
    } catch (IOException ex) {
      throw new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
    }
  }

  private PersistentMemberID bytesToPMID(byte[] bytes) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais);
      PersistentMemberID result = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(result, dis);
      return result;
    } catch (IOException io) {
      throw new DiskAccessException(
          String.format("Failed to read file during recovery from %s",
              this.ifFile.getPath()),
          io, this.parent);
    } catch (ClassNotFoundException cnf) {
      throw new DiskAccessException(
          String.format("Failed to read file during recovery from %s",
              this.ifFile.getPath()),
          cnf, this.parent);
    }
  }

  // non-private methods

  DiskInitFile(String name, DiskStoreImpl parent, boolean shouldExist, Set<File> oplogs) {
    this.parent = parent;
    File f = new File(this.parent.getInfoFileDir().getDir(), "BACKUP" + name + IF_FILE_EXT);
    final boolean didNotExist = !f.exists();
    if (shouldExist && didNotExist) {
      String msg = String.format("The init file %s does not exist.",
          new Object[] {f});
      if (!oplogs.isEmpty()) {
        Set<File> allOplogs = new LinkedHashSet(oplogs);
        msg +=
            String.format(
                "If it no longer exists then delete the following files to be able to create this disk store. Existing oplogs are: %s",
                new Object[] {allOplogs});
      }
      throw new IllegalStateException(msg);
    }
    this.ifFile = f;
    this.dsIds = new IntOpenHashSet();
    this.instIds = new IntOpenHashSet();
    this.crfIds = new LongOpenHashSet();
    this.drfIds = new LongOpenHashSet();
    this.krfIds = new ConcurrentHashSet<>();
    recover();
    if (this.parent.isOffline() && !this.parent.isOfflineCompacting()
        && !this.parent.isOfflineModify()) {
      dump();
    }
    openRAF();
    if (!this.parent.isOffline() || this.parent.isOfflineCompacting()) {
      if (didNotExist) {
        this.parent.setDiskStoreID(DiskStoreID.random());
        writeDiskStoreId();
        saveGemfireVersion(); // normal create diskstore
      }
      this.regListener = new InternalDataSerializer.RegistrationListener() {
        @Override
        public void newInstantiator(Instantiator i) {
          saveInstantiator(i);
        }

        @Override
        public void newDataSerializer(DataSerializer ds) {
          saveDataSerializer(ds);
        }
      };
      InternalDataSerializer.addRegistrationListener(this.regListener);
      // do this after the listener is registered to make sure we don't
      // miss any registrations.
      saveInstantiators();
      saveDataSerializers();
    } else {
      this.regListener = null;
    }
  }

  void closeRegion(DiskRegionView dr) {
    lock(true);
    try {
      this.parent.rmById(dr.getId()); // fix for bug 41334
      PlaceHolderDiskRegion phdr = new PlaceHolderDiskRegion(dr);
      this.drMap.put(dr.getId(), phdr);
      this.drMapByName.put(dr.getName(), phdr);
      // @todo make sure we only have one instance of the region for this name
    } finally {
      unlock(true);
    }
  }

  void clearRegion(DiskRegionView dr, long clearOplogEntryId) {
    lock(true);
    try {
      if (clearOplogEntryId != DiskStoreImpl.INVALID_ID) {
        this.ifTotalRecordCount++;
        if (dr.getClearOplogEntryId() == DiskStoreImpl.INVALID_ID) {
          this.ifLiveRecordCount++;
        } else {
          // we now have one record to gc (the previous clear).
        }
        dr.setClearOplogEntryId(clearOplogEntryId);
        if (clearOplogEntryId > clearOplogEntryIdHWM) {
          clearOplogEntryIdHWM = clearOplogEntryId;
        }
        writeIFRecord(IFREC_CLEAR_REGION_ID, dr, clearOplogEntryId);
      }
    } finally {
      unlock(true);
    }
  }

  /**
   * Clear the region using an RVV.
   */
  void clearRegion(DiskRegion dr, RegionVersionVector rvv) {
    lock(true);
    try {
      this.ifTotalRecordCount++;
      if (dr.getClearRVV() == null) {
        this.ifLiveRecordCount++;
      } else {
        // we now have one record to gc (the previous clear).
      }
      dr.setClearRVV(rvv);
      writeClearRecord(dr, rvv);
    } finally {
      unlock(true);
    }

  }

  /**
   * Write a clear with an RVV record.
   */
  private void writeClearRecord(DiskRegionView dr, RegionVersionVector rvv) {
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
      hdos.write(IFREC_CLEAR_REGION_WITH_RVV_ID);
      writeDiskRegionID(hdos, dr.getId());
      // We only need the memberToVersionMap for clear purposes
      Map<DiskStoreID, RegionVersionHolder> memberToVersion = rvv.getMemberToVersion();
      hdos.writeInt(memberToVersion.size());
      for (Map.Entry<DiskStoreID, RegionVersionHolder> entry : memberToVersion.entrySet()) {
        InternalDataSerializer.invokeToData(entry.getKey(), hdos);
        synchronized (entry.getValue()) {
          InternalDataSerializer.invokeToData(entry.getValue(), hdos);
        }
      }
      hdos.write(END_OF_RECORD_ID);
      writeIFRecord(hdos, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    }

  }

  void createRegion(DiskRegionView drv) {
    lock(true);
    try {
      if (!drv.isRecreated()) {
        writeIFRecord(IFREC_CREATE_REGION_ID, drv, drv.getName());
        this.liveRegions++;
        writeRegionConfig(drv);
        // no need to add to drMap since it will be in the DiskStore drMap
      } else {
        if (drv.hasConfigChanged()) {
          writeRegionConfig(drv);
          drv.setConfigChanged(false);
        }
      }
    } finally {
      unlock(true);
    }
  }

  void beginDestroyRegion(DiskRegionView dr) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        cmnBeginDestroyRegion(dr);
        writeIFRecord(IFREC_BEGIN_DESTROY_REGION_ID, dr);
      }
    } finally {
      unlock(true);
    }
  }

  void endDestroyRegion(DiskRegionView dr) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        cmnEndDestroyRegion(dr);
        writeIFRecord(IFREC_END_DESTROY_REGION_ID, dr);
        if (logger.isDebugEnabled()) {
          logger.trace(LogMarker.PERSIST_WRITES_VERBOSE,
              "DiskInitFile IFREC_END_DESTROY_REGION_ID drId={}", dr.getId());
        }
      }
    } finally {
      unlock(true);
    }
  }

  void beginDestroyDataStorage(DiskRegionView dr) {
    lock(true);
    try {
      assert regionStillCreated(dr);
      cmnBeginPartialDestroyRegion(dr);
      writeIFRecord(IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID, dr);
    } finally {
      unlock(true);
    }
  }

  void endDestroyDataStorage(DiskRegionView dr) {
    lock(true);
    try {
      assert regionStillCreated(dr);
      cmnEndPartialDestroyRegion(dr);
      writeIFRecord(IFREC_END_PARTIAL_DESTROY_REGION_ID, dr);
    } finally {
      unlock(true);
    }
  }

  public void createPersistentPR(String name, PRPersistentConfig config) {
    lock(true);
    try {
      if (cmnPRCreate(name, config)) {
        writePRCreate(name, config);
      }
    } finally {
      unlock(true);
    }
  }

  public void destroyPersistentPR(String name) {
    lock(true);
    try {
      if (cmnPRDestroy(name)) {
        writePRDestroy(name);
      }
    } finally {
      unlock(true);
    }
  }

  public PRPersistentConfig getPersistentPR(String name) {
    lock(false);
    try {
      return prMap.get(name);
    } finally {
      unlock(false);
    }
  }

  public Map<String, PRPersistentConfig> getAllPRs() {
    lock(false);
    try {
      return new HashMap<String, PRPersistentConfig>(prMap);
    } finally {
      unlock(false);
    }
  }

  void crfCreate(long oplogId) {
    lock(true);
    try {
      cmnCrfCreate(oplogId);
      writeIFRecord(IFREC_CRF_CREATE, oplogId);
    } finally {
      unlock(true);
    }
  }

  void drfCreate(long oplogId) {
    lock(true);
    try {
      cmnDrfCreate(oplogId);
      writeIFRecord(IFREC_DRF_CREATE, oplogId);
    } finally {
      unlock(true);
    }
  }

  void krfCreate(long oplogId) {
    lock(true);
    try {
      cmnKrfCreate(oplogId);
      writeIFRecord(IFREC_KRF_CREATE, oplogId);
    } finally {
      unlock(true);
    }
  }

  void crfDelete(long oplogId) {
    lock(true);
    try {
      if (cmnCrfDelete(oplogId)) {
        // call writeIFRecord AFTER cmnCrfDelete to fix bug 41505
        writeIFRecord(IFREC_CRF_DELETE, oplogId);
      }
    } finally {
      unlock(true);
    }
  }

  void drfDelete(long oplogId) {
    lock(true);
    try {
      if (cmnDrfDelete(oplogId)) {
        writeIFRecord(IFREC_DRF_DELETE, oplogId);
      }
    } finally {
      unlock(true);
    }
  }

  int getOrCreateCanonicalId(Object object) {
    lock(true);
    try {
      int id = canonicalIdHolder.getId(object);
      if (id <= 0) {
        id = canonicalIdHolder.createId(object);
        writeCanonicalId(id, object);
      }
      return id;
    } finally {
      unlock(true);
    }
  }

  Object getCanonicalObject(int id) {
    lock(false);
    try {
      return canonicalIdHolder.getObject(id);
    } finally {
      unlock(false);
    }
  }

  void close() {
    lock(true);
    try {
      if (this.closed)
        return;
      this.closed = true;
      stopListeningForDataSerializerChanges();
      try {
        this.ifRAF.close();
      } catch (IOException ignore) {
      }
      for (DiskRegionView k : this.getKnown()) {
        k.close();
      }
      if (this.liveRegions == 0 && !parent.isValidating()) {
        basicDestroy();
      }
    } finally {
      unlock(true);
    }
  }

  void destroy() {
    lock(true);
    try {
      close();
      basicDestroy();
    } finally {
      unlock(true);
    }
  }

  private void basicDestroy() {
    if (this.ifFile.exists()) {
      if (!this.ifFile.delete()) {
        if (logger.isDebugEnabled()) {
          logger.debug("could not delete file {}", this.ifFile);
        }
      }
    }
  }


  void addMyInitializingPMID(DiskRegionView dr, PersistentMemberID pmid) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        cmnAddMyInitializingPMID(dr, pmid);
        writePMIDRecord(IFREC_MY_MEMBER_INITIALIZING_ID, dr, pmid, false);
      }
    } finally {
      unlock(true);
    }
  }

  void markInitialized(DiskRegionView dr) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        writeIFRecord(IFREC_MY_MEMBER_INITIALIZED_ID, dr);
        cmnMarkInitialized(dr);
      }
    } finally {
      unlock(true);
    }
  }

  void addOnlinePMID(DiskRegionView dr, PersistentMemberID pmid) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        if (dr.addOnlineMember(pmid)) {
          if (dr.rmOfflineMember(pmid) || dr.rmEqualMember(pmid)) {
            this.ifLiveRecordCount--;
          }
          writePMIDRecord(IFREC_ONLINE_MEMBER_ID, dr, pmid, true);
        }
      }
    } finally {
      unlock(true);
    }
  }

  void addOfflinePMID(DiskRegionView dr, PersistentMemberID pmid) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        if (dr.addOfflineMember(pmid)) {
          if (dr.rmOnlineMember(pmid) || dr.rmEqualMember(pmid)) {
            this.ifLiveRecordCount--;
          }
          writePMIDRecord(IFREC_OFFLINE_MEMBER_ID, dr, pmid, true);
        }
      }
    } finally {
      unlock(true);
    }
  }

  void addOfflineAndEqualPMID(DiskRegionView dr, PersistentMemberID pmid) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        if (dr.addOfflineAndEqualMember(pmid)) {
          if (dr.rmOnlineMember(pmid) || dr.rmOfflineMember(pmid)) {
            this.ifLiveRecordCount--;
          }
          writePMIDRecord(IFREC_OFFLINE_AND_EQUAL_MEMBER_ID, dr, pmid, true);
        }
      }
    } finally {
      unlock(true);
    }
  }

  void rmPMID(DiskRegionView dr, PersistentMemberID pmid) {
    lock(true);
    try {
      if (regionStillCreated(dr)) {
        if (dr.rmOnlineMember(pmid) || dr.rmOfflineMember(pmid) || dr.rmEqualMember(pmid)) {
          // we now have two records to gc (this one and the live one we removed).
          this.ifLiveRecordCount--;
          this.ifTotalRecordCount++;
          writePMIDRecord(IFREC_RM_MEMBER_ID, dr, pmid, false);
        }
      }
    } finally {
      unlock(true);
    }
  }

  public void revokeMember(PersistentMemberPattern revokedPattern) {
    // We're only going to record members revoked with the new API -
    // using the UUID
    if (revokedPattern.getUUID() == null) {
      return;
    }

    lock(true);
    try {
      if (cmnRevokeDiskStoreId(revokedPattern)) {
        // we now have two records to gc (this one and the live one we removed).
        this.ifLiveRecordCount++;
        this.ifTotalRecordCount++;
        writeRevokedMember(revokedPattern);
      }
    } finally {
      unlock(true);
    }
  }

  /**
   * Get the set of members known to be revoked
   */
  public Set<PersistentMemberPattern> getRevokedIDs() {
    lock(false);
    try {
      // Return a copy of the set, because we modify it in place.
      return new HashSet<PersistentMemberPattern>(this.revokedMembers);
    } finally {
      unlock(false);
    }

  }

  /**
   * Return true if the given dr is still created in this IF.
   */
  boolean regionStillCreated(DiskRegionView dr) {
    lock(false);
    try {
      return getDiskRegionById(dr.getId()) != null;
    } finally {
      unlock(false);
    }
  }

  boolean regionExists(long drId) {
    lock(false);
    try {
      // @todo make drMap concurrent so this call can be fast
      return this.drMap.containsKey(drId);
    } finally {
      unlock(false);
    }
  }

  Collection<DiskRegionView> getKnown() {
    lock(false);
    try {
      return new ArrayList<DiskRegionView>(this.drMap.values());
    } finally {
      unlock(false);
    }
  }

  @Override
  public void cmnAddMyInitializingPMID(long drId, PersistentMemberID pmid) {
    cmnAddMyInitializingPMID(getDiskRegionById(drId), pmid);

  }

  @Override
  public void cmnBeginDestroyRegion(long drId) {
    cmnBeginDestroyRegion(getDiskRegionById(drId));
  }

  @Override
  public void cmnBeginPartialDestroyRegion(long drId) {
    cmnBeginPartialDestroyRegion(getDiskRegionById(drId));
  }

  @Override
  public void cmnEndDestroyRegion(long drId) {
    cmnEndDestroyRegion(getDiskRegionById(drId));
  }

  @Override
  public void cmnEndPartialDestroyRegion(long drId) {
    cmnEndPartialDestroyRegion(getDiskRegionById(drId));
  }

  @Override
  public void cmnMarkInitialized(long drId) {
    cmnMarkInitialized(getDiskRegionById(drId));
  }

  @Override
  public void cmnDiskStoreID(DiskStoreID diskStoreID) {
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "diskStoreId={}", diskStoreID);
    }
    this.parent.setDiskStoreID(diskStoreID);
  }

  @Override
  public boolean cmnRevokeDiskStoreId(PersistentMemberPattern revokedPattern) {
    return this.revokedMembers.add(revokedPattern);
  }

  @Override
  public String getNameForError() {
    return this.parent.toString();
  }

  @Override
  public boolean isClosing() {
    return parent.isClosing();
  }

  public void dump() {
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      System.out.println("expectedCrfs=" + Arrays.toString(this.crfIds.toArray()));
      System.out.println("expectedDrfs=" + Arrays.toString(this.drfIds.toArray()));
      System.out.println("dataSerializerIds=" + Arrays.toString(this.dsIds.toArray()));
      System.out.println("instantiatorIds=  " + Arrays.toString(this.instIds.toArray()));
    }
  }

  /**
   * Returns a map of region_name->(pr_buckets|replicated_region)
   */
  private Map<String, List<PlaceHolderDiskRegion>> getRegionsToDump(String regName) {
    if (regName == null) {
      Map<String, List<PlaceHolderDiskRegion>> regions =
          new HashMap<String, List<PlaceHolderDiskRegion>>();
      for (DiskRegionView drv : this.drMap.values()) {
        if (drv instanceof PlaceHolderDiskRegion) {
          PlaceHolderDiskRegion dr = (PlaceHolderDiskRegion) drv;
          if (dr.isBucket()) {
            List<PlaceHolderDiskRegion> buckets = regions.get(dr.getPrName());
            if (buckets == null) {
              buckets = new ArrayList<PlaceHolderDiskRegion>();
              regions.put(dr.getPrName(), buckets);
            }
            buckets.add(dr);
          } else {
            regions.put(drv.getName(), Collections.singletonList(dr));
          }
        }
      }
      return regions;
    } else {
      DiskRegionView drv = getDiskRegionByName(regName);
      if (drv == null) {
        List<PlaceHolderDiskRegion> buckets = new ArrayList<PlaceHolderDiskRegion>();
        for (PlaceHolderDiskRegion dr : this.drMapByName.values()) {
          if (dr.isBucket()) {
            if (dr.getName().equals(dr.getPrName())) {
              buckets.add(dr);
            }
          }
        }
        if (buckets.isEmpty()) {
          throw new IllegalArgumentException(
              "The disk store does not contain a region named " + regName);
        } else {
          return Collections.singletonMap(regName, buckets);
        }
      } else if (drv instanceof PlaceHolderDiskRegion) {
        return Collections.singletonMap(regName,
            Collections.singletonList((PlaceHolderDiskRegion) drv));
      } else {
        return Collections.emptyMap();
      }
    }
  }

  public void dumpRegionInfo(PrintStream printStream, String regName) {
    printStream.println("Regions in the disk store:");
    for (Map.Entry<String, List<PlaceHolderDiskRegion>> regionEntry : getRegionsToDump(regName)
        .entrySet()) {
      printStream.print("  ");
      List<PlaceHolderDiskRegion> regions = regionEntry.getValue();
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        for (PlaceHolderDiskRegion region : regions) {
          region.dump(printStream);
        }
      } else {
        // NOTE, regions will always have at least 1 item.
        regions.get(0).dump(printStream);
      }
    }
  }

  public void dumpRegionMetadata(boolean showBuckets) {
    System.out.println("Disk Store ID: " + getDiskStore().getDiskStoreID());
    System.out.println("Regions in the disk store:");
    for (Map.Entry<String, List<PlaceHolderDiskRegion>> regionEntry : getRegionsToDump(null)
        .entrySet()) {
      System.out.print("  ");
      List<PlaceHolderDiskRegion> regions = regionEntry.getValue();
      PlaceHolderDiskRegion region0 = regions.get(0);
      if (region0.isBucket()) {
        dumpPRMetaData(showBuckets, regions);

      } else {
        region0.dumpMetadata();
      }
    }
  }

  /**
   * Dump the metadata for a partitioned region, optionally dumping the meta data for individual
   * buckets.
   */
  private void dumpPRMetaData(boolean showBuckets, List<PlaceHolderDiskRegion> regions) {
    StringBuilder msg = new StringBuilder(regions.get(0).getPrName());
    regions.get(0).dumpCommonAttributes(msg);

    if (showBuckets) {
      for (PlaceHolderDiskRegion region : regions) {
        msg.append("\n");
        msg.append("\n");
        msg.append(region.getName());
        region.dumpPersistentView(msg);
      }
    } else {
      Map<DiskStoreID, String> online = new HashMap<DiskStoreID, String>();
      Map<DiskStoreID, String> offline = new HashMap<DiskStoreID, String>();
      Map<DiskStoreID, String> equal = new HashMap<DiskStoreID, String>();
      for (PlaceHolderDiskRegion region : regions) {
        for (PersistentMemberID mem : region.getOnlineMembers()) {
          online.put(mem.getDiskStoreId(), mem.getHost() + ":" + mem.getDirectory());
        }
        for (PersistentMemberID mem : region.getOfflineMembers()) {
          offline.put(mem.getDiskStoreId(), mem.getHost() + ":" + mem.getDirectory());
        }
        for (PersistentMemberID mem : region.getOfflineAndEqualMembers()) {
          equal.put(mem.getDiskStoreId(), mem.getHost() + ":" + mem.getDirectory());
        }
      }

      msg.append("\n\tonlineMembers:");
      for (Map.Entry<DiskStoreID, String> id : online.entrySet()) {
        msg.append("\n\t\t").append(id.getKey()).append(" ").append(id.getValue());
      }

      msg.append("\n\tofflineMembers:");
      for (Map.Entry<DiskStoreID, String> id : offline.entrySet()) {
        msg.append("\n\t\t").append(id.getKey()).append(" ").append(id.getValue());
      }

      msg.append("\n\tequalsMembers:");
      for (Map.Entry<DiskStoreID, String> id : equal.entrySet()) {
        msg.append("\n\t\t").append(id.getKey()).append(" ").append(id.getValue());
      }
    }

    System.out.println(msg);
  }

  public void destroyPRRegion(String prName) {
    ArrayList<PlaceHolderDiskRegion> buckets = new ArrayList<PlaceHolderDiskRegion>();
    lock(true);
    try {
      for (PlaceHolderDiskRegion dr : this.drMapByName.values()) {
        if (dr.isBucket()) {
          if (prName.equals(dr.getPrName())) {
            buckets.add(dr);
          }
        }
      }
    } finally {
      unlock(true);
    }
    for (PlaceHolderDiskRegion dr : buckets) {
      endDestroyRegion(dr);
    }

    // Remove the partitioned region record
    // for this disk store.
    destroyPersistentPR(prName);
  }

  public String modifyPRRegion(String prName, String lruOption, String lruActionOption,
      String lruLimitOption, String concurrencyLevelOption, String initialCapacityOption,
      String loadFactorOption, String compressorClassNameOption, String statisticsEnabledOption,
      String offHeapOption, boolean printToConsole) {
    StringBuffer sb = new StringBuffer();
    ArrayList<PlaceHolderDiskRegion> buckets = new ArrayList<PlaceHolderDiskRegion>();
    lock(true);
    try {
      for (PlaceHolderDiskRegion dr : this.drMapByName.values()) {
        if (dr.isBucket()) {
          if (prName.equals(dr.getPrName())) {
            buckets.add(dr);
          }
        }
      }

      // only print info on the first bucket to fix bug 41735
      boolean printInfo = true;
      for (PlaceHolderDiskRegion dr : buckets) {
        String message = basicModifyRegion(printInfo, dr, lruOption, lruActionOption,
            lruLimitOption, concurrencyLevelOption, initialCapacityOption, loadFactorOption,
            compressorClassNameOption, statisticsEnabledOption, offHeapOption, printToConsole);
        if (printInfo)
          sb.append(message);
        printInfo = false;
      }
    } finally {
      unlock(true);
    }
    return sb.toString();
  }

  public String modifyRegion(DiskRegionView drv, String lruOption, String lruActionOption,
      String lruLimitOption, String concurrencyLevelOption, String initialCapacityOption,
      String loadFactorOption, String compressorClassNameOption, String statisticsEnabledOption,
      String offHeapOption, boolean printToConsole) {
    lock(true);
    try {
      return basicModifyRegion(false, drv, lruOption, lruActionOption, lruLimitOption,
          concurrencyLevelOption, initialCapacityOption, loadFactorOption,
          compressorClassNameOption, statisticsEnabledOption, offHeapOption, printToConsole);
    } finally {
      unlock(true);
    }
  }

  private String basicModifyRegion(boolean printInfo, DiskRegionView drv, String lruOption,
      String lruActionOption, String lruLimitOption, String concurrencyLevelOption,
      String initialCapacityOption, String loadFactorOption, String compressorClassNameOption,
      String statisticsEnabledOption, String offHeapOption, boolean printToConsole) {
    byte lruAlgorithm = drv.getLruAlgorithm();
    byte lruAction = drv.getLruAction();
    int lruLimit = drv.getLruLimit();
    int concurrencyLevel = drv.getConcurrencyLevel();
    int initialCapacity = drv.getInitialCapacity();
    float loadFactor = drv.getLoadFactor();
    String compressorClassName = drv.getCompressorClassName();
    boolean statisticsEnabled = drv.getStatisticsEnabled();
    boolean offHeap = drv.getOffHeap();
    StringBuffer sb = new StringBuffer();
    final String lineSeparator = System.getProperty("line.separator");

    if (lruOption != null) {
      EvictionAlgorithm ea = EvictionAlgorithm.parseAction(lruOption);
      if (ea != null) {
        lruAlgorithm = (byte) ea.getValue();
      } else {
        throw new IllegalArgumentException(
            "Expected lru to be one of the following: \"none\", \"lru-entry-count\", \"lru-heap-percentage\", or \"lru-memory-size\"");
      }
      if (ea.isNone()) {
        lruAction = (byte) EvictionAction.NONE.getValue();
        lruLimit = 0;
      } else if (ea.isLRUHeap()) {
        lruLimit = 0;
      }
    }
    if (lruActionOption != null) {
      EvictionAction ea = EvictionAction.parseAction(lruActionOption);
      if (ea != null) {
        lruAction = (byte) ea.getValue();
      } else {
        throw new IllegalArgumentException(
            "Expected lruAction to be one of the following: \"none\", \"overflow-to-disk\", or \"local-destroy\"");
      }
    }
    if (lruLimitOption != null) {
      lruLimit = Integer.parseInt(lruLimitOption);
      if (lruLimit < 0) {
        throw new IllegalArgumentException("Expected lruLimit to be greater than or equal to zero");
      }
    }
    if (concurrencyLevelOption != null) {
      concurrencyLevel = Integer.parseInt(concurrencyLevelOption);
      if (concurrencyLevel < 0) {
        throw new IllegalArgumentException(
            "Expected concurrencyLevel to be greater than or equal to zero");
      }
    }
    if (initialCapacityOption != null) {
      initialCapacity = Integer.parseInt(initialCapacityOption);
      if (initialCapacity < 0) {
        throw new IllegalArgumentException(
            "Expected initialCapacity to be greater than or equal to zero");
      }
    }
    if (loadFactorOption != null) {
      loadFactor = Float.parseFloat(loadFactorOption);
      if (loadFactor < 0.0) {
        throw new IllegalArgumentException(
            "Expected loadFactor to be greater than or equal to zero");
      }
    }
    if (compressorClassNameOption != null) {
      compressorClassName =
          (compressorClassNameOption.isEmpty() ? null : compressorClassNameOption);
    }
    if (statisticsEnabledOption != null) {
      statisticsEnabled = Boolean.parseBoolean(statisticsEnabledOption);
      if (!statisticsEnabled) {
        // make sure it is "false"
        if (!statisticsEnabledOption.equalsIgnoreCase("false")) {
          throw new IllegalArgumentException(
              "Expected statisticsEnabled to be \"true\" or \"false\"");
        }
      }
    }
    if (offHeapOption != null) {
      offHeap = Boolean.parseBoolean(offHeapOption);
      if (!offHeap) {
        // make sure it is "false"
        if (!offHeapOption.equalsIgnoreCase("false")) {
          throw new IllegalArgumentException("Expected offHeap to be \"true\" or \"false\"");
        }
      }
    }

    sb.append("Before modification: ");
    sb.append(lineSeparator);
    sb.append(((PlaceHolderDiskRegion) drv).dump2());
    sb.append(lineSeparator);

    drv.setConfig(lruAlgorithm, lruAction, lruLimit, concurrencyLevel, initialCapacity, loadFactor,
        statisticsEnabled, drv.isBucket(), drv.getFlags(), drv.getPartitionName(),
        drv.getStartingBucketId(), compressorClassName, offHeap);

    // Make sure the combined lru args can still produce a legal eviction attributes
    // before writing them to disk.
    ((PlaceHolderDiskRegion) drv).getEvictionAttributes();
    writeRegionConfig(drv);

    sb.append("After modification: ");
    sb.append(lineSeparator);
    sb.append(((PlaceHolderDiskRegion) drv).dump2());
    sb.append(lineSeparator);

    String message = sb.toString();

    if (printInfo && printToConsole) {
      System.out.println(message);
    }
    return message;
  }

  private void writeGemfireVersion(Version version) {
    lock(true);
    try {
      ByteBuffer bb = getIFWriteBuffer(1 + 3 + 1);
      bb.put(IFREC_GEMFIRE_VERSION);
      Version.writeOrdinal(bb, version.ordinal(), false);
      bb.put(END_OF_RECORD_ID);
      writeIFRecord(bb, false); // don't do stats for these small records
    } catch (IOException ex) {
      DiskAccessException dae = new DiskAccessException(
          String.format("Failed writing data to initialization file because: %s", ex),
          this.parent);
      if (!this.compactInProgress) {
        this.parent.handleDiskAccessException(dae);
      }
      throw dae;
    } finally {
      unlock(true);
    }
  }

  /**
   * Additional flags for a disk region that are persisted in its meta-data.
   *
   * @since GemFire 7.0
   */
  public enum DiskRegionFlag {
    /**
     * True if this disk region has entries with versioning enabled. Depending on this flag, the
     * appropriate RegionEntryFactory gets instantiated.
     */
    IS_WITH_VERSIONING
  }
}
