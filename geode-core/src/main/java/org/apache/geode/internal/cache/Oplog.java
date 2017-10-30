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
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.distributed.OplogCancelledException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.Flushable;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.DiskStoreImpl.OplogCompactor;
import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import org.apache.geode.internal.cache.DistributedRegion.DiskPosition;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.persistence.BytesAndBits;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.internal.cache.persistence.UninterruptibleRandomAccessFile;
import org.apache.geode.internal.cache.versions.CompactVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.internal.shared.NativeCalls;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.internal.util.TransformUtils;
import org.apache.geode.pdx.internal.PdxWriterImpl;

/**
 * Implements an operation log to write to disk. As of prPersistSprint2 this file only supports
 * persistent regions. For overflow only regions see {@link OverflowOplog}.
 * 
 * @since GemFire 5.1
 */
public class Oplog implements CompactableOplog, Flushable {
  private static final Logger logger = LogService.getLogger();

  /** Extension of the oplog file * */
  public static final String CRF_FILE_EXT = ".crf";
  public static final String DRF_FILE_EXT = ".drf";
  public static final String KRF_FILE_EXT = ".krf";

  /** The file which will be created on disk * */
  private File diskFile;

  /** boolean marked true when this oplog is closed * */
  private volatile boolean closed;

  private final OplogFile crf = new OplogFile();
  private final OplogFile drf = new OplogFile();
  private final KRFile krf = new KRFile();

  /** The stats for this store */
  private final DiskStoreStats stats;

  /** The store that owns this Oplog* */
  private final DiskStoreImpl parent;

  /**
   * The oplog set this oplog is part of
   */
  private final PersistentOplogSet oplogSet;

  /** oplog id * */
  protected final long oplogId;

  /** recovered gemfire version * */
  protected Version gfversion;

  /**
   * Recovered version of the data. Usually this is same as {@link #gfversion} except for the case
   * of upgrading disk store from previous version in which case the keys/values are carried forward
   * as is and need to be interpreted in load by latest product code if required.
   */
  protected Version dataVersion;

  /** Directory in which the file is present* */
  private DirectoryHolder dirHolder;

  /** The max Oplog size (user configurable) * */
  private final long maxOplogSize;
  private long maxCrfSize;
  private long maxDrfSize;

  private final AtomicBoolean hasDeletes = new AtomicBoolean();

  private boolean firstRecord = true;

  /**
   * This system property instructs that writes be synchronously written to disk and not to file
   * system. (Use rwd instead of rw - RandomAccessFile property)
   */
  private static final boolean SYNC_WRITES =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "syncWrites");

  /**
   * The HighWaterMark of recentValues.
   */
  private final AtomicLong totalCount = new AtomicLong(0);

  /**
   * The number of records in this oplog that contain the most recent value of the entry.
   */
  private final AtomicLong totalLiveCount = new AtomicLong(0);

  private final ConcurrentMap<Long, DiskRegionInfo> regionMap =
      new ConcurrentHashMap<Long, DiskRegionInfo>();

  /**
   * Set to true once compact is called on this oplog.
   * 
   * @since GemFire prPersistSprint1
   */
  private volatile boolean compacting = false;

  /**
   * Set to true after the first drf recovery.
   */
  private boolean haveRecoveredDrf = true;

  /**
   * Set to true after the first crf recovery.
   */
  private boolean haveRecoveredCrf = true;

  private OpState opState;

  /**
   * Written to CRF, and DRF.
   */
  private static final byte OPLOG_EOF_ID = 0;
  private static final byte END_OF_RECORD_ID = 21;

  /**
   * Written to CRF and DRF. Followed by 16 bytes which is the leastSigBits and mostSigBits of a
   * UUID for the disk store we belong to. 1: EndOfRecord Is written once at the beginning of every
   * oplog file.
   */
  private static final byte OPLOG_DISK_STORE_ID = 62;
  static final int OPLOG_DISK_STORE_REC_SIZE = 1 + 16 + 1;

  /**
   * Written to CRF. Followed by 8 bytes which is the BASE_ID to use for any NEW_ENTRY records. 1:
   * EndOfRecord Only needs to be written once per oplog and must preceed any OPLOG_NEW_ENTRY_0ID
   * records.
   * 
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_NEW_ENTRY_BASE_ID = 63;

  static final int OPLOG_NEW_ENTRY_BASE_REC_SIZE = 1 + 8 + 1;

  /**
   * Written to CRF. The OplogEntryId is +1 the previous new_entry OplogEntryId. Byte Format: 1:
   * userBits RegionId 4: valueLength (optional depending on bits) valueLength: value bytes
   * (optional depending on bits) 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_NEW_ENTRY_0ID = 64;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 1 byte. Byte Format: 1: userBits 1: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_1ID = 65;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 2 bytes. Byte Format: 1: userBits 2: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_2ID = 66;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 3 bytes. Byte Format: 1: userBits 3: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_3ID = 67;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 4 bytes. Byte Format: 1: userBits 4: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_4ID = 68;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 5 bytes. Byte Format: 1: userBits 5: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_5ID = 69;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 6 bytes. Byte Format: 1: userBits 6: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_6ID = 70;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 7 bytes. Byte Format: 1: userBits 7: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_7ID = 71;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 8 bytes. Byte Format: 1: userBits 8: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_8ID = 72;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 1 byte. Byte Format: 1: userBits 1: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_1ID = 73;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 2 bytes. Byte Format: 1: userBits 2: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_2ID = 74;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 3 bytes. Byte Format: 1: userBits 3: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_3ID = 75;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 4 bytes. Byte Format: 1: userBits 4: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_4ID = 76;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 5 bytes. Byte Format: 1: userBits 5: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_5ID = 77;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 6 bytes. Byte Format: 1: userBits 6: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_6ID = 78;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 7 bytes. Byte Format: 1: userBits 7: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_7ID = 79;

  /**
   * Written to CRF. The OplogEntryId is relative to the previous mod_entry OplogEntryId. The signed
   * difference is encoded in 8 bytes. Byte Format: 1: userBits 8: OplogEntryId RegionId 4:
   * valueLength (optional depending on bits) valueLength: value bytes (optional depending on bits)
   * 4: keyLength keyLength: key bytes 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_MOD_ENTRY_WITH_KEY_8ID = 80;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 1 byte. Byte Format: 1: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_1ID = 81;
  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 2 bytes. Byte Format: 2: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_2ID = 82;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 3 bytes. Byte Format: 3: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_3ID = 83;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 4 bytes. Byte Format: 4: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_4ID = 84;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 5 bytes. Byte Format: 5: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_5ID = 85;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 6 bytes. Byte Format: 6: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_6ID = 86;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 7 bytes. Byte Format: 7: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_7ID = 87;

  /**
   * Written to DRF. The OplogEntryId is relative to the previous del_entry OplogEntryId. The signed
   * difference is encoded in 8 bytes. Byte Format: 8: OplogEntryId 1: EndOfRecord
   *
   * @since GemFire prPersistSprint1
   */
  private static final byte OPLOG_DEL_ENTRY_8ID = 88;

  /**
   * The maximum size of a DEL_ENTRY record in bytes. Currenty this is 10; 1 for opcode and 8 for
   * oplogEntryId and 1 for END_OF_RECORD_ID
   */
  private static final int MAX_DELETE_ENTRY_RECORD_BYTES = 1 + 8 + 1;

  /**
   * Written to beginning of each CRF. Contains the RVV for all regions in the CRF. Byte Format 8:
   * number of regions (variable length encoded number) for each region 4: number of members
   * (variable length encoded number) for each member 4: canonical member id (variable length
   * encoded number) 8: version id (variable length encoded number) 4: number of exceptions
   * (variable length encoded number) variable: exceptions
   */
  private static final byte OPLOG_RVV = 89;

  /**
   * When detected conflict, besides persisting the golden copy by modify(), also persist the
   * conflict operation's region version and member id. and failedWritten to beginning of each CRF.
   * Contains the RVV for all regions in the CRF. Byte Format regionId versions
   */
  private static final byte OPLOG_CONFLICT_VERSION = 90;

  /**
   * persist Gemfire version string into crf, drf, krf Byte Format variable gemfire version string,
   * such as 7.0.0.beta EndOfRecord
   */
  private static final byte OPLOG_GEMFIRE_VERSION = 91;

  static final int OPLOG_GEMFIRE_VERSION_REC_SIZE = 1 + 3 + 1;

  /**
   * Persist oplog file magic number. Written once at the beginning of every oplog file; CRF, DRF,
   * KRF, IF and IRF. Followed by 6 byte magic number. Each oplog type has a different magic number
   * Followed by EndOfRecord Fix for bug 43824
   * 
   * @since GemFire 8.0
   */
  static final byte OPLOG_MAGIC_SEQ_ID = 92;

  public enum OPLOG_TYPE {
    CRF(new byte[] {0x47, 0x46, 0x43, 0x52, 0x46, 0x31}), // GFCRF1
    DRF(new byte[] {0x47, 0x46, 0x44, 0x52, 0x46, 0x31}), // GFDRF1
    IRF(new byte[] {0x47, 0x46, 0x49, 0x52, 0x46, 0x31}), // GFIRF1
    KRF(new byte[] {0x47, 0x46, 0x4b, 0x52, 0x46, 0x31}), // GFKRF1
    IF(new byte[] {0x47, 0x46, 0x49, 0x46, 0x30, 0x31}); // GFIF01

    private final byte[] bytes;

    OPLOG_TYPE(byte[] byteSeq) {
      this.bytes = byteSeq;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public static int getLen() {
      return 6;
    }
  }

  public static final int OPLOG_MAGIC_SEQ_REC_SIZE = 1 + OPLOG_TYPE.getLen() + 1;

  /** Compact this oplogs or no. A client configurable property * */
  private final boolean compactOplogs;

  /**
   * This object is used to correctly identify the OpLog size so as to cause a switch of oplogs
   */
  final Object lock = new Object();

  final ByteBuffer[] bbArray = new ByteBuffer[2];

  private boolean lockedForKRFcreate = false;

  /**
   * Set to true when this oplog will no longer be written to. Never set to false once it becomes
   * true.
   */
  private boolean doneAppending = false;

  /**
   * Creates new {@code Oplog} for the given region.
   * 
   * @param oplogId int identifying the new oplog
   * @param dirHolder The directory in which to create new Oplog
   * 
   * @throws DiskAccessException if the disk files can not be initialized
   */
  Oplog(long oplogId, PersistentOplogSet parent, DirectoryHolder dirHolder) {
    if (oplogId > DiskId.MAX_OPLOG_ID) {
      throw new IllegalStateException(
          "Too many oplogs. The oplog id can not exceed " + DiskId.MAX_OPLOG_ID);
    }
    this.oplogId = oplogId;
    this.oplogSet = parent;
    this.parent = parent.getParent();
    this.dirHolder = dirHolder;
    // Pretend we have already seen the first record.
    // This will cause a large initial record to force a switch
    // which allows the maxDirSize to be checked.
    this.firstRecord = false;
    this.opState = new OpState();
    long maxOplogSizeParam = getParent().getMaxOplogSizeInBytes();
    long availableSpace = this.dirHolder.getAvailableSpace();
    if (availableSpace < maxOplogSizeParam) {
      if (DiskStoreImpl.PREALLOCATE_OPLOGS && !DiskStoreImpl.SET_IGNORE_PREALLOCATE) {
        throw new DiskAccessException(
            LocalizedStrings.Oplog_PreAllocate_Failure_Init.toLocalizedString(this.dirHolder,
                maxOplogSizeParam),
            new IOException("not enough space left to create and pre grow oplog files, available="
                + availableSpace + ", required=" + maxOplogSizeParam),
            getParent());
      }
      this.maxOplogSize = availableSpace;
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Reducing maxOplogSize to {} because that is all the room remaining in the directory.",
            availableSpace);
      }
    } else {
      this.maxOplogSize = maxOplogSizeParam;
    }
    setMaxCrfDrfSize();
    this.stats = getParent().getStats();
    this.compactOplogs = getParent().getAutoCompact();

    this.closed = false;
    String n = getParent().getName();
    this.diskFile = new File(this.dirHolder.getDir(), oplogSet.getPrefix() + n + "_" + oplogId);
    try {
      createDrf(null);
      createCrf(null);
      // open krf for offline compaction
      if (getParent().isOfflineCompacting()) {
        krfFileCreate();
      }
    } catch (Exception ex) {
      close();

      getParent().getCancelCriterion().checkCancelInProgress(ex);
      if (ex instanceof DiskAccessException) {
        throw (DiskAccessException) ex;
      }
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_CREATING_OPERATION_LOG_BECAUSE_0.toLocalizedString(ex),
          getParent());
    }
  }

  /**
   * A copy constructor used for creating a new oplog based on the previous Oplog. This constructor
   * is invoked only from the function switchOplog
   * 
   * @param oplogId integer identifying the new oplog
   * @param dirHolder The directory in which to create new Oplog
   * @param prevOplog The previous oplog
   */
  private Oplog(long oplogId, DirectoryHolder dirHolder, Oplog prevOplog) {
    if (oplogId > DiskId.MAX_OPLOG_ID) {
      throw new IllegalStateException(
          "Too many oplogs. The oplog id can not exceed " + DiskId.MAX_OPLOG_ID);
    }
    this.oplogId = oplogId;
    this.parent = prevOplog.parent;
    this.oplogSet = prevOplog.oplogSet;
    this.dirHolder = dirHolder;
    this.opState = new OpState();
    long maxOplogSizeParam = getParent().getMaxOplogSizeInBytes();
    long availableSpace = this.dirHolder.getAvailableSpace();
    if (prevOplog.compactOplogs) {
      this.maxOplogSize = maxOplogSizeParam;
    } else {
      if (availableSpace < maxOplogSizeParam) {
        this.maxOplogSize = availableSpace;
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Reducing maxOplogSize to {} because that is all the room remaining in the directory.",
              availableSpace);
        }
      } else {
        this.maxOplogSize = maxOplogSizeParam;
      }
    }
    setMaxCrfDrfSize();
    this.stats = prevOplog.stats;
    this.compactOplogs = prevOplog.compactOplogs;
    // copy over the previous Oplog's data version since data is not being
    // transformed at this point
    this.dataVersion = prevOplog.getDataVersionIfOld();

    this.closed = false;
    String n = getParent().getName();
    this.diskFile = new File(this.dirHolder.getDir(), oplogSet.getPrefix() + n + "_" + oplogId);
    try {
      createDrf(prevOplog.drf);
      createCrf(prevOplog.crf);
      // open krf for offline compaction
      if (getParent().isOfflineCompacting()) {
        krfFileCreate();
      }
    } catch (Exception ex) {
      close();

      getParent().getCancelCriterion().checkCancelInProgress(ex);
      if (ex instanceof DiskAccessException) {
        throw (DiskAccessException) ex;
      }
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_CREATING_OPERATION_LOG_BECAUSE_0.toLocalizedString(ex),
          getParent());
    }
  }

  public void replaceIncompatibleEntry(DiskRegionView dr, DiskEntry old, DiskEntry repl) {
    boolean useNextOplog = false;
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock) {
      if (getOplogSet().getChild() != this) {
        // make sure to only call replaceIncompatibleEntry for child, because
        // this.lock
        // can only sync with compaction thread on child oplog
        useNextOplog = true;
      } else {
        // This method is use in recovery only and will not be called by
        // compaction.
        // It's only called before or after compaction. It will replace
        // DiskEntry
        // in DiskRegion without modifying DiskId (such as to a new oplogId),
        // Not to change the entry count in oplog either. While doing that,
        // this.lock will lock the current child to sync with compaction thread.
        // If replace thread got this.lock, DiskEntry "old" will not be removed
        // from
        // current oplog (maybe not child). If compaction thread got this.lock,
        // DiskEntry "old" should have been moved to child oplog when replace
        // thread
        // processes it.

        // See #48032. A new region entry has been put into the region map, but
        // we
        // also have to replace it in the oplog live entries that are used to
        // write
        // the krf. If we don't, we will recover the wrong (old) value.
        getOrCreateDRI(dr).replaceLive(old, repl);
        if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
          logger.trace(LogMarker.PERSIST_RECOVERY,
              "replacing incompatible entry key = {} old = {} new = {} oldDiskId = {} new diskId = {} tag = {} in child oplog #{}",
              old.getKey(), System.identityHashCode(old), System.identityHashCode(repl),
              old.getDiskId(), repl.getDiskId(), old.getVersionStamp(), this.getOplogId());
        }
      }
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(getOplogSet().getChild() != this);
      getOplogSet().getChild().replaceIncompatibleEntry(dr, old, repl);
    }
  }

  private void writeDiskStoreRecord(OplogFile olf, OPLOG_TYPE type) throws IOException {
    this.opState = new OpState();
    this.opState.initialize(type);
    writeOpLogBytes(olf, false, true);
    olf.currSize += getOpStateSize();
    this.dirHolder.incrementTotalOplogSize(getOpStateSize());
    clearOpState();

    this.opState.initialize(getParent().getDiskStoreID());
    writeOpLogBytes(olf, false, true); // fix for bug 41928
    olf.currSize += getOpStateSize();
    this.dirHolder.incrementTotalOplogSize(getOpStateSize());
  }

  private void writeGemfireVersionRecord(OplogFile olf) throws IOException {
    if (this.gfversion == null) {
      this.gfversion = Version.CURRENT;
    }
    Version dataVersion = getDataVersionIfOld();
    if (dataVersion == null) {
      dataVersion = Version.CURRENT;
    }
    // if gfversion and dataVersion are not same, then write a special token
    // version and then write both, else write gfversion as before
    // this is for backward compatibility with 7.0
    this.opState = new OpState();
    if (this.gfversion == dataVersion) {
      writeProductVersionRecord(this.gfversion, olf);
    } else {
      writeProductVersionRecord(Version.TOKEN, olf);
      clearOpState();
      writeProductVersionRecord(this.gfversion, olf);
      clearOpState();
      writeProductVersionRecord(dataVersion, olf);
    }
  }

  private void writeProductVersionRecord(Version version, OplogFile olf) throws IOException {
    this.opState.initialize(version.ordinal());
    writeOpLogBytes(olf, false, true);
    olf.currSize += getOpStateSize();
    this.dirHolder.incrementTotalOplogSize(getOpStateSize());
  }

  public Version currentRecoveredGFVersion() {
    return this.gfversion;
  }

  /**
   * Write an RVV record containing all of the live disk regions.
   */
  private void writeRVVRecord(OplogFile olf, boolean writeGCRVV) throws IOException {
    writeRVVRecord(olf, getParent().getAllDiskRegions(), writeGCRVV);
  }

  /**
   * Write the RVV record for the given regions.
   * 
   * @param olf the oplog to write to
   * @param diskRegions the set of disk regions we should write the RVV of
   * @param writeGCRVV true to write write the GC RVV
   */
  private void writeRVVRecord(OplogFile olf, Map<Long, AbstractDiskRegion> diskRegions,
      boolean writeGCRVV) throws IOException {
    this.opState = new OpState();
    this.opState.initialize(diskRegions, writeGCRVV);
    writeOpLogBytes(olf, false, true); // fix for bug 41928
    olf.currSize += getOpStateSize();
    this.dirHolder.incrementTotalOplogSize(getOpStateSize());
  }

  private boolean wroteNewEntryBase = false;

  /**
   * Write a OPLOG_NEW_ENTRY_BASE_ID to this oplog. Must be called before any OPLOG_NEW_ENTRY_0ID
   * records are written to this oplog.
   */
  private boolean writeNewEntryBaseRecord(boolean async) throws IOException {
    if (this.wroteNewEntryBase)
      return false;
    this.wroteNewEntryBase = true;
    long newEntryBase = getOplogSet().getOplogEntryId();

    OpState saved = this.opState;
    try {
      this.opState = new OpState();
      this.opState.initialize(newEntryBase);
      writeOpLogBytes(this.crf, async, false/* no need to flush this record */);
      this.dirHolder.incrementTotalOplogSize(getOpStateSize());
    } finally {
      this.opState = saved;
    }
    return true;
  }

  /**
   * Return true if this oplog has a drf but does not have a crf
   */
  boolean isDrfOnly() {
    return this.drf.f != null && this.crf.f == null;
  }

  /**
   * This constructor will get invoked only in case of persistent region when it is recovering an
   * oplog.
   */
  Oplog(long oplogId, PersistentOplogSet parent) {
    // @todo have the crf and drf use different directories.
    if (oplogId > DiskId.MAX_OPLOG_ID) {
      throw new IllegalStateException(
          "Too many oplogs. The oplog id can not exceed " + DiskId.MAX_OPLOG_ID);
    }
    this.isRecovering = true;
    this.oplogId = oplogId;
    this.parent = parent.getParent();
    this.oplogSet = parent;
    this.opState = new OpState();
    this.maxOplogSize = getParent().getMaxOplogSizeInBytes();
    setMaxCrfDrfSize();
    this.stats = getParent().getStats();
    this.compactOplogs = getParent().getAutoCompact();
    this.closed = true;
    this.crf.RAFClosed = true;
    this.deleted.set(true);
    this.haveRecoveredCrf = false;
    this.haveRecoveredDrf = false;
  }

  /**
   * Returns true if added file was crf; false if drf
   */
  boolean addRecoveredFile(File f, DirectoryHolder dh) {
    String fname = f.getName();
    if (this.dirHolder != null) {
      if (!dh.equals(this.dirHolder)) {
        throw new DiskAccessException(
            "Oplog#" + getOplogId() + " has files in two different directories: \"" + this.dirHolder
                + "\", and \"" + dh
                + "\". Both the crf and drf for this oplog should be in the same directory.",
            getParent());
      }
    } else {
      this.dirHolder = dh;
    }
    if (fname.endsWith(Oplog.CRF_FILE_EXT)) {
      this.crf.f = f;
      return true;
    } else if (fname.endsWith(Oplog.DRF_FILE_EXT)) {
      this.drf.f = f;
      // } else if (fname.endsWith(Oplog.KRF_FILE_EXT)) {
      // this.krf.f = f;
    } else {
      assert false : fname;
    }
    return false;
  }

  void setRecoveredDrfSize(long size) {
    this.drf.currSize += size;
    this.drf.bytesFlushed += size;
  }

  void setRecoveredCrfSize(long size) {
    this.crf.currSize += size;
    this.crf.bytesFlushed += size;
  }

  private boolean isRecovering;

  boolean isRecovering() {
    return this.isRecovering;
  }

  private DiskStoreImpl getParent() {
    return this.parent;
  }

  private PersistentOplogSet getOplogSet() {
    return oplogSet;
  }

  void initAfterRecovery(boolean offline) {
    this.isRecovering = false;
    this.closed = false;
    this.deleted.set(false);
    String n = getParent().getName();
    // crf might not exist; but drf always will
    this.diskFile =
        new File(this.drf.f.getParentFile(), oplogSet.getPrefix() + n + "_" + this.oplogId);
    try {
      // This is a recovered oplog and we only read from its crf.
      // No need to open the drf.
      this.doneAppending = true;
      if (this.crf.f != null && !hasNoLiveValues()) {
        this.closed = false;
        // truncate crf/drf if their actual size is less than their pre-blow
        // size
        this.crf.raf = new UninterruptibleRandomAccessFile(this.crf.f, "rw");
        this.crf.RAFClosed = false;
        this.crf.channel = this.crf.raf.getChannel();
        unpreblow(this.crf, getMaxCrfSize());
        this.crf.raf.close();
        // make crf read only
        this.crf.raf = new UninterruptibleRandomAccessFile(this.crf.f, "r");
        this.crf.channel = this.crf.raf.getChannel();
        this.stats.incOpenOplogs();

        // drf.raf is null at this point. create one and close it to retain
        // existing behavior
        try {
          this.drf.raf = new UninterruptibleRandomAccessFile(this.drf.f, "rw");
          this.drf.RAFClosed = false;
          this.drf.channel = this.drf.raf.getChannel();
          unpreblow(this.drf, getMaxDrfSize());
        } finally {
          this.drf.raf.close();
          this.drf.raf = null;
          this.drf.RAFClosed = true;
        }
        // no need to seek to the end; we will not be writing to a recovered
        // oplog; only reading
        // this.crf.raf.seek(this.crf.currSize);
      } else if (!offline) {
        // drf exists but crf has been deleted (because it was empty).
        // I don't think the drf needs to be opened. It is only used during
        // recovery.
        // At some point the compacter my identify that it can be deleted.
        this.crf.RAFClosed = true;
        deleteCRF();
        this.closed = true;
        this.deleted.set(true);
      }
      this.drf.RAFClosed = true; // since we never open it on a recovered oplog
    } catch (IOException ex) {
      getParent().getCancelCriterion().checkCancelInProgress(ex);
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_CREATING_OPERATION_LOG_BECAUSE_0.toLocalizedString(ex),
          getParent());
    }
    if (hasNoLiveValues() && !offline) {
      getOplogSet().removeOplog(getOplogId(), true, getHasDeletes() ? this : null);
      if (!getHasDeletes()) {
        getOplogSet().drfDelete(this.oplogId);
        deleteFile(this.drf);
      }
    } else if (needsCompaction()) {
      // just leave it in the list it is already in
    } else {
      // remove it from the compactable list
      getOplogSet().removeOplog(getOplogId(),
          true/*
               * say we are deleting so that undeletedOplogSize is not inced
               */, null);
      // add it to the inactive list
      getOplogSet().addInactive(this);
    }
  }

  boolean getHasDeletes() {
    return this.hasDeletes.get();
  }

  private void setHasDeletes(boolean v) {
    this.hasDeletes.set(v);
  }

  private void closeAndDeleteAfterEx(IOException ex, OplogFile olf) {
    if (olf == null) {
      return;
    }

    if (olf.raf != null) {
      try {
        olf.raf.close();
      } catch (IOException e) {
        logger.warn(
            LocalizedMessage.create(LocalizedStrings.Oplog_Close_Failed, olf.f.getAbsolutePath()),
            e);
      }
    }
    olf.RAFClosed = true;
    if (!olf.f.delete() && olf.f.exists()) {
      throw new DiskAccessException(
          LocalizedStrings.Oplog_COULD_NOT_DELETE__0_.toLocalizedString(olf.f.getAbsolutePath()),
          ex, getParent());
    }
  }

  private void preblow(OplogFile olf, long maxSize) throws IOException {
    long availableSpace = this.dirHolder.getAvailableSpace();
    if (availableSpace >= maxSize) {
      try {
        NativeCalls.getInstance().preBlow(olf.f.getAbsolutePath(), maxSize,
            (DiskStoreImpl.PREALLOCATE_OPLOGS && !DiskStoreImpl.SET_IGNORE_PREALLOCATE));
      } catch (IOException ioe) {
        if (logger.isDebugEnabled()) {
          logger.debug("Could not pregrow oplog to {} because: {}", maxSize, ioe.getMessage(), ioe);
        }
        // I don't think I need any of this. If setLength throws then
        // the file is still ok.
        // I need this on windows. I'm seeing this in testPreblowErrorCondition:
        // Caused by: java.io.IOException: The parameter is incorrect
        // at sun.nio.ch.FileDispatcher.write0(Native Method)
        // at sun.nio.ch.FileDispatcher.write(FileDispatcher.java:44)
        // at sun.nio.ch.IOUtil.writeFromNativeBuffer(IOUtil.java:104)
        // at sun.nio.ch.IOUtil.write(IOUtil.java:60)
        // at sun.nio.ch.FileChannelImpl.write(FileChannelImpl.java:206)
        // at org.apache.geode.internal.cache.Oplog.flush(Oplog.java:3377)
        // at
        // org.apache.geode.internal.cache.Oplog.flushAll(Oplog.java:3419)
        /*
         * { String os = System.getProperty("os.name"); if (os != null) { if (os.indexOf("Windows")
         * != -1) { olf.raf.close(); olf.RAFClosed = true; if (!olf.f.delete() && olf.f.exists()) {
         * throw new DiskAccessException
         * (LocalizedStrings.Oplog_COULD_NOT_DELETE__0_.toLocalizedString (olf.f.getAbsolutePath()),
         * getParent()); } olf.raf = new RandomAccessFile(olf.f, SYNC_WRITES ? "rwd" : "rw");
         * olf.RAFClosed = false; } } }
         */
        closeAndDeleteAfterEx(ioe, olf);
        throw new DiskAccessException(LocalizedStrings.Oplog_PreAllocate_Failure
            .toLocalizedString(olf.f.getAbsolutePath(), maxSize), ioe, getParent());
      }
    }
    // TODO: Perhaps the test flag is not requierd here. Will re-visit.
    else if (DiskStoreImpl.PREALLOCATE_OPLOGS && !DiskStoreImpl.SET_IGNORE_PREALLOCATE) {
      throw new DiskAccessException(
          LocalizedStrings.Oplog_PreAllocate_Failure.toLocalizedString(olf.f.getAbsolutePath(),
              maxSize),
          new IOException("not enough space left to pre-blow, available=" + availableSpace
              + ", required=" + maxSize),
          getParent());
    }
  }

  private void unpreblow(OplogFile olf, long maxSize) {
    synchronized (/* olf */this.lock) {
      if (!olf.RAFClosed && !olf.unpreblown) {
        olf.unpreblown = true;
        if (olf.currSize < maxSize) {
          try {
            olf.raf.setLength(olf.currSize);
          } catch (IOException ignore) {
          }
        }
      }
    }
  }

  /**
   * Creates the crf oplog file
   */
  private void createCrf(OplogFile prevOlf) throws IOException {
    File f = new File(this.diskFile.getPath() + CRF_FILE_EXT);

    if (logger.isDebugEnabled()) {
      logger.debug("Creating operation log file {}", f);
    }
    this.crf.f = f;
    preblow(this.crf, getMaxCrfSize());
    this.crf.raf = new UninterruptibleRandomAccessFile(f, SYNC_WRITES ? "rwd" : "rw");
    this.crf.RAFClosed = false;
    oplogSet.crfCreate(this.oplogId);
    this.crf.writeBuf = allocateWriteBuf(prevOlf);
    logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_CREATE_0_1_2,
        new Object[] {toString(), getFileType(this.crf), getParent().getName()}));
    this.crf.channel = this.crf.raf.getChannel();

    this.stats.incOpenOplogs();
    writeDiskStoreRecord(this.crf, OPLOG_TYPE.CRF);
    writeGemfireVersionRecord(this.crf);
    writeRVVRecord(this.crf, false);

    // Fix for bug 41654 - don't count the header
    // size against the size of the oplog. This ensures that
    // even if we have a large RVV, we can still write up to
    // max-oplog-size bytes to this oplog.
    this.maxCrfSize += this.crf.currSize;
  }

  private static ByteBuffer allocateWriteBuf(OplogFile prevOlf) {
    if (prevOlf != null && prevOlf.writeBuf != null) {
      ByteBuffer result = prevOlf.writeBuf;
      prevOlf.writeBuf = null;
      return result;
    } else {
      return ByteBuffer.allocateDirect(Integer.getInteger("WRITE_BUF_SIZE", 32768));
    }
  }

  /**
   * Creates the drf oplog file
   */
  private void createDrf(OplogFile prevOlf) throws IOException {
    File f = new File(this.diskFile.getPath() + DRF_FILE_EXT);
    this.drf.f = f;
    if (logger.isDebugEnabled()) {
      logger.debug("Creating operation log file {}", f);
    }
    preblow(this.drf, getMaxDrfSize());
    this.drf.raf = new UninterruptibleRandomAccessFile(f, SYNC_WRITES ? "rwd" : "rw");
    this.drf.RAFClosed = false;
    this.oplogSet.drfCreate(this.oplogId);
    this.drf.writeBuf = allocateWriteBuf(prevOlf);
    logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_CREATE_0_1_2,
        new Object[] {toString(), getFileType(this.drf), getParent().getName()}));
    this.drf.channel = this.drf.raf.getChannel();
    writeDiskStoreRecord(this.drf, OPLOG_TYPE.DRF);
    writeGemfireVersionRecord(this.drf);
    writeRVVRecord(this.drf, true);
  }

  /**
   * Returns the {@code DiskStoreStats} for this oplog
   */
  public DiskStoreStats getStats() {
    return this.stats;
  }

  /**
   * Test Method to be used only for testing purposes. Gets the underlying File object for the Oplog
   * . Oplog class uses this File object to obtain the RandomAccessFile object. Before returning the
   * File object , the dat present in the buffers of the RandomAccessFile object is flushed.
   * Otherwise, for windows the actual file length does not match with the File size obtained from
   * the File object
   */
  File getOplogFileForTest() throws IOException {
    // @todo check callers for drf
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* crf */) {
      if (!this.crf.RAFClosed) {
        this.crf.raf.getFD().sync();
      }
      return this.crf.f;
    }
  }

  File getCrfFile() {
    return this.crf.f;
  }

  File getDrfFile() {
    return this.drf.f;
  }

  /**
   * Given a set of Oplog file names return a Set of the oplog files that match those names that are
   * managed by this Oplog.
   * 
   * @param oplogFileNames a Set of operation log file names.
   */
  Set<String> gatherMatchingOplogFiles(Set<String> oplogFileNames) {
    Set<String> matchingFiles = new HashSet<>();

    // CRF match?
    if ((null != this.crf.f) && this.crf.f.exists()
        && oplogFileNames.contains(this.crf.f.getName())) {
      matchingFiles.add(this.crf.f.getName());
    }

    // DRF match?
    if ((null != this.drf.f) && this.drf.f.exists()
        && oplogFileNames.contains(this.drf.f.getName())) {
      matchingFiles.add(this.drf.f.getName());
    }

    // KRF match?
    if (getParent().getDiskInitFile().hasKrf(this.oplogId)) {
      File krfFile = getKrfFile();
      if (krfFile.exists() && oplogFileNames.contains(krfFile.getName())) {
        matchingFiles.add(krfFile.getName());
      }
    }

    return matchingFiles;
  }

  /**
   * Returns a map of baseline oplog files to copy that match this oplog's files for a currently
   * running backup.
   * 
   * @param baselineOplogFiles a List of files to match this oplog's filenames against.
   * @return a map of baslineline oplog files to copy. May be empty if total current set for this
   *         oplog does not match the baseline.
   */
  Map<File, File> mapBaseline(Collection<File> baselineOplogFiles) {
    // Map of baseline oplog file name to oplog file
    Map<String, File> baselineOplogMap =
        TransformUtils.transformAndMap(baselineOplogFiles, TransformUtils.fileNameTransformer);

    // Returned Map of baseline file to current oplog file
    Map<File, File> baselineToOplogMap = new HashMap<>();

    // Check for crf existence
    if ((null != this.crf.f) && this.crf.f.exists()
        && baselineOplogMap.containsKey(this.crf.f.getName())) {
      baselineToOplogMap.put(baselineOplogMap.get(this.crf.f.getName()),
          IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(this.crf.f));
    }

    // Check for drf existence
    if ((null != this.drf.f) && this.drf.f.exists()
        && baselineOplogMap.containsKey(this.drf.f.getName())) {
      baselineToOplogMap.put(baselineOplogMap.get(this.drf.f.getName()),
          IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(this.drf.f));
    }

    // Check for krf existence
    if (getParent().getDiskInitFile().hasKrf(this.oplogId)) {
      File krfFile = getKrfFile();
      if (krfFile.exists() && baselineOplogMap.containsKey(krfFile.getName())) {
        baselineToOplogMap.put(baselineOplogMap.get(krfFile.getName()),
            IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(krfFile));
      }
    }

    return baselineToOplogMap;
  }

  /** the oplog identifier * */
  public long getOplogId() {
    return this.oplogId;
  }

  /**
   * Returns the unserialized bytes and bits for the given Entry. If Oplog is destroyed while
   * querying, then the DiskRegion is queried again to obatin the value This method should never get
   * invoked for an entry which has been destroyed
   * 
   * @since GemFire 3.2.1
   * @param id The DiskId for the entry @param offset The offset in this OpLog where the entry is
   *        present. @param faultingIn @param bitOnly boolean indicating whether to extract just the
   *        UserBit or UserBit with value @return BytesAndBits object wrapping the value & user bit
   */
  public BytesAndBits getBytesAndBits(DiskRegionView dr, DiskId id, boolean faultingIn,
      boolean bitOnly) {
    Oplog retryOplog = null;
    long offset = 0;
    synchronized (id) {
      long opId = id.getOplogId();
      if (opId != getOplogId()) {
        // the oplog changed on us so we need to do a recursive
        // call after unsyncing
        retryOplog = getOplogSet().getChild(opId);
      } else {
        // fetch this while synced so it will be consistent with oplogId
        offset = id.getOffsetInOplog();
      }
    }
    if (retryOplog != null) {
      return retryOplog.getBytesAndBits(dr, id, faultingIn, bitOnly);
    }
    BytesAndBits bb = null;
    long start = this.stats.startRead();

    // If the offset happens to be -1, still it is possible that
    // the data is present in the current oplog file.
    if (offset == -1) {
      // Since it is given that a get operation has alreadty
      // taken a
      // lock on an entry , no put operation could have modified the
      // oplog ID
      // there fore synchronization is not needed
      offset = id.getOffsetInOplog();
    }

    // If the current OpLog is not destroyed ( its opLogRaf file
    // is still open) we can retrieve the value from this oplog.
    try {
      bb = basicGet(dr, offset, bitOnly, id.getValueLength(), id.getUserBits());
    } catch (DiskAccessException dae) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.Oplog_OPLOGBASICGET_ERROR_IN_READING_THE_DATA_FROM_DISK_FOR_DISK_ID_HAVING_DATA_AS_0,
          id), dae);
      throw dae;
    }

    if (bb == null) {
      throw new EntryDestroyedException(
          LocalizedStrings.Oplog_NO_VALUE_WAS_FOUND_FOR_ENTRY_WITH_DISK_ID_0_ON_A_REGION_WITH_SYNCHRONOUS_WRITING_SET_TO_1
              .toLocalizedString(new Object[] {id, dr.isSync()}));
    }
    if (bitOnly) {
      dr.endRead(start, this.stats.endRead(start, 1), 1);
    } else {
      dr.endRead(start, this.stats.endRead(start, bb.getBytes().length), bb.getBytes().length);
    }
    return bb;

  }

  /**
   * Returns the object stored on disk with the given id. This method is used for testing purposes
   * only. As such, it bypasses the buffer and goes directly to the disk. This is not a thread safe
   * function , in the sense, it is possible that by the time the OpLog is queried , data might move
   * HTree with the oplog being destroyed
   * 
   * @param id A DiskId object for which the value on disk will be fetched
   */
  public BytesAndBits getNoBuffer(DiskRegion dr, DiskId id) {
    if (logger.isDebugEnabled()) {
      logger.debug("Oplog::getNoBuffer:Before invoking Oplog.basicGet for DiskID ={}", id);
    }

    try {
      return basicGet(dr, id.getOffsetInOplog(), false, id.getValueLength(), id.getUserBits());
    } catch (DiskAccessException dae) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.Oplog_OPLOGGETNOBUFFEREXCEPTION_IN_RETRIEVING_VALUE_FROM_DISK_FOR_DISKID_0,
          id), dae);
      throw dae;
    } catch (IllegalStateException ise) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.Oplog_OPLOGGETNOBUFFEREXCEPTION_IN_RETRIEVING_VALUE_FROM_DISK_FOR_DISKID_0,
          id), ise);
      throw ise;
    }
  }

  void close(DiskRegion dr) {
    // while a krf is being created can not close a region
    lockCompactor();
    try {
      addUnrecoveredRegion(dr.getId());
      DiskRegionInfo dri = getDRI(dr);
      if (dri != null) {
        long clearCount = dri.clear(null);
        if (clearCount != 0) {
          this.totalLiveCount.addAndGet(-clearCount);
          // no need to call handleNoLiveValues because we now have an
          // unrecovered region.
        }
        this.regionMap.remove(dr.getId(), dri);
      }
    } finally {
      unlockCompactor();
    }
  }

  void clear(DiskRegion dr, RegionVersionVector rvv) {
    DiskRegionInfo dri = getDRI(dr);
    if (dri != null) {
      long clearCount = dri.clear(rvv);
      if (clearCount != 0) {
        this.totalLiveCount.addAndGet(-clearCount);
        if (!isCompacting() || calledByCompactorThread()) {
          handleNoLiveValues();
        }
      }
    }
  }

  void destroy(DiskRegion dr) {
    DiskRegionInfo dri = getDRI(dr);
    if (dri != null) {
      long clearCount = dri.clear(null);
      if (clearCount != 0) {
        this.totalLiveCount.addAndGet(-clearCount);
        if (!isCompacting() || calledByCompactorThread()) {
          handleNoLiveValues();
        }
      }
      this.regionMap.remove(dr.getId(), dri);
    }
  }

  long getMaxRecoveredOplogEntryId() {
    long result = this.recoverNewEntryId;
    if (this.recoverModEntryIdHWM > result) {
      result = this.recoverModEntryIdHWM;
    }
    if (this.recoverDelEntryIdHWM > result) {
      result = this.recoverDelEntryIdHWM;
    }
    return result;
  }

  /**
   * Used during recovery to calculate the OplogEntryId of the next NEW_ENTRY record.
   * 
   * @since GemFire prPersistSprint1
   */
  private long recoverNewEntryId = DiskStoreImpl.INVALID_ID;
  /**
   * Used during writing to remember the last MOD_ENTRY OplogEntryId written to this oplog.
   * 
   * @since GemFire prPersistSprint1
   */
  private long writeModEntryId = DiskStoreImpl.INVALID_ID;
  /**
   * Used during recovery to calculate the OplogEntryId of the next MOD_ENTRY record.
   * 
   * @since GemFire prPersistSprint1
   */
  private long recoverModEntryId = DiskStoreImpl.INVALID_ID;
  /**
   * Added to fix bug 41301. High water mark of modified entries.
   */
  private long recoverModEntryIdHWM = DiskStoreImpl.INVALID_ID;
  /**
   * Added to fix bug 41340. High water mark of deleted entries.
   */
  private long recoverDelEntryIdHWM = DiskStoreImpl.INVALID_ID;
  /**
   * Used during writing to remember the last DEL_ENTRY OplogEntryId written to this oplog.
   * 
   * @since GemFire prPersistSprint1
   */
  private long writeDelEntryId = DiskStoreImpl.INVALID_ID;
  /**
   * Used during recovery to calculate the OplogEntryId of the next DEL_ENTRY record.
   * 
   * @since GemFire prPersistSprint1
   */
  private long recoverDelEntryId = DiskStoreImpl.INVALID_ID;

  private void setRecoverNewEntryId(long v) {
    this.recoverNewEntryId = v;
  }

  private long incRecoverNewEntryId() {
    this.recoverNewEntryId += 1;
    return this.recoverNewEntryId;
  }

  /**
   * Given a delta calculate the OplogEntryId for a MOD_ENTRY.
   */
  public long calcModEntryId(long delta) {
    long oplogKeyId = this.recoverModEntryId + delta;
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY,
          "calcModEntryId delta={} recoverModEntryId={}  oplogKeyId={}", delta,
          this.recoverModEntryId, oplogKeyId);
    }
    this.recoverModEntryId = oplogKeyId;
    if (oplogKeyId > this.recoverModEntryIdHWM) {
      this.recoverModEntryIdHWM = oplogKeyId; // fixes bug 41301
    }
    return oplogKeyId;
  }

  /**
   * Given a delta calculate the OplogEntryId for a DEL_ENTRY.
   */
  public long calcDelEntryId(long delta) {
    long oplogKeyId = this.recoverDelEntryId + delta;
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY,
          "calcDelEntryId delta={} recoverModEntryId={}  oplogKeyId={}", delta,
          this.recoverModEntryId, oplogKeyId);
    }
    this.recoverDelEntryId = oplogKeyId;
    if (oplogKeyId > this.recoverDelEntryIdHWM) {
      this.recoverDelEntryIdHWM = oplogKeyId; // fixes bug 41340
    }
    return oplogKeyId;
  }

  private boolean crashed;

  boolean isCrashed() {
    return this.crashed;
  }

  /**
   * Return bytes read.
   */
  long recoverDrf(OplogEntryIdSet deletedIds, boolean alreadyRecoveredOnce, boolean latestOplog) {
    File drfFile = this.drf.f;
    if (drfFile == null) {
      this.haveRecoveredDrf = true;
      return 0L;
    }
    lockCompactor();
    try {
      if (this.haveRecoveredDrf && !getHasDeletes())
        return 0L; // do this while holding lock
      if (!this.haveRecoveredDrf) {
        this.haveRecoveredDrf = true;
      }
      logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_RECOVERING_OPLOG_0_1_2,
          new Object[] {toString(), drfFile.getAbsolutePath(), getParent().getName()}));
      this.recoverDelEntryId = DiskStoreImpl.INVALID_ID;
      boolean readLastRecord = true;
      CountingDataInputStream dis = null;
      try {
        int recordCount = 0;
        boolean foundDiskStoreRecord = false;
        FileInputStream fis = null;
        try {
          fis = new FileInputStream(drfFile);
          dis = new CountingDataInputStream(new BufferedInputStream(fis, 32 * 1024),
              drfFile.length());
          boolean endOfLog = false;
          while (!endOfLog) {
            if (dis.atEndOfFile()) {
              endOfLog = true;
              break;
            }
            readLastRecord = false;
            byte opCode = dis.readByte();
            if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
              logger.trace(LogMarker.PERSIST_RECOVERY, "drf byte={} location={}", opCode,
                  Long.toHexString(dis.getCount()));
            }
            switch (opCode) {
              case OPLOG_EOF_ID:
                // we are at the end of the oplog. So we need to back up one byte
                dis.decrementCount();
                endOfLog = true;
                break;
              case OPLOG_DEL_ENTRY_1ID:
              case OPLOG_DEL_ENTRY_2ID:
              case OPLOG_DEL_ENTRY_3ID:
              case OPLOG_DEL_ENTRY_4ID:
              case OPLOG_DEL_ENTRY_5ID:
              case OPLOG_DEL_ENTRY_6ID:
              case OPLOG_DEL_ENTRY_7ID:
              case OPLOG_DEL_ENTRY_8ID:
                readDelEntry(dis, opCode, deletedIds, parent);
                recordCount++;
                break;
              case OPLOG_DISK_STORE_ID:
                readDiskStoreRecord(dis, this.drf.f);
                foundDiskStoreRecord = true;
                recordCount++;
                break;
              case OPLOG_MAGIC_SEQ_ID:
                readOplogMagicSeqRecord(dis, this.drf.f, OPLOG_TYPE.DRF);
                break;
              case OPLOG_GEMFIRE_VERSION:
                readGemfireVersionRecord(dis, this.drf.f);
                recordCount++;
                break;

              case OPLOG_RVV:
                long idx = dis.getCount();
                readRVVRecord(dis, this.drf.f, true, latestOplog);
                recordCount++;
                break;

              default:
                throw new DiskAccessException(
                    LocalizedStrings.Oplog_UNKNOWN_OPCODE_0_FOUND_IN_DISK_OPERATION_LOG
                        .toLocalizedString(opCode),
                    getParent());
            }
            readLastRecord = true;
            // @todo
            // if (rgn.isDestroyed()) {
            // break;
            // }
          } // while
        } finally {
          if (dis != null) {
            dis.close();
          }
          if (fis != null) {
            fis.close();
          }
        }
        if (!foundDiskStoreRecord && recordCount > 0) {
          throw new DiskAccessException(
              "The oplog file \"" + this.drf.f + "\" does not belong to the init file \""
                  + getParent().getInitFile() + "\". Drf did not contain a disk store id.",
              getParent());
        }
      } catch (EOFException ignore) {
        // ignore since a partial record write can be caused by a crash
      } catch (IOException ex) {
        getParent().getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_READING_FILE_DURING_RECOVERY_FROM_0
                .toLocalizedString(drfFile.getPath()),
            ex, getParent());
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Oplog::readOplog:Error in recovery as Cache was closed", e);
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Oplog::readOplog:Error in recovery as Region was destroyed", e);
        }
      } catch (IllegalStateException e) {
        throw e;
      }
      // Add the Oplog size to the Directory Holder which owns this oplog,
      // so that available space is correctly calculated & stats updated.
      long byteCount = 0;
      if (!readLastRecord) {
        // this means that there was a crash
        // and hence we should not continue to read
        // the next oplog
        this.crashed = true;
        if (dis != null) {
          byteCount = dis.getFileLength();
        }
      } else {
        if (dis != null) {
          byteCount = dis.getCount();
        }
      }
      if (!alreadyRecoveredOnce) {
        setRecoveredDrfSize(byteCount);
        this.dirHolder.incrementTotalOplogSize(byteCount);
      }
      return byteCount;
    } finally {
      unlockCompactor();
    }
  }

  /**
   * This map is used during recovery to keep track of what entries were recovered. Its keys are the
   * oplogEntryId; its values are the actual logical keys that end up in the Region's keys. It used
   * to be a local variable in basicInitializeOwner but now that it needs to live longer than that
   * method I made it an instance variable It is now only alive during recoverRegionsThatAreReady so
   * it could once again be passed down into each oplog.
   * <p>
   * If offlineCompaction the value in this map will have the key bytes, values bytes, user bits,
   * etc (any info we need to copy forward).
   */
  private OplogEntryIdMap kvMap;

  private OplogEntryIdMap getRecoveryMap() {
    return this.kvMap;
  }

  /**
   * This map is used during recover to keep track of keys that are skipped. Later modify records in
   * the same oplog may use this map to retrieve the correct key.
   */
  private OplogEntryIdMap skippedKeyBytes;

  private boolean readKrf(OplogEntryIdSet deletedIds, boolean recoverValues,
      boolean recoverValuesSync, Set<Oplog> oplogsNeedingValueRecovery, boolean latestOplog) {
    File f = new File(this.diskFile.getPath() + KRF_FILE_EXT);
    if (!f.exists()) {
      return false;
    }

    if (!getParent().getDiskInitFile().hasKrf(this.oplogId)) {
      logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_REMOVING_INCOMPLETE_KRF,
          new Object[] {f.getName(), this.oplogId, getParent().getName()}));
      f.delete();
    }
    // Set krfCreated to true since we have a krf.
    this.krfCreated.set(true);

    // Fix for 42741 - we do this after creating setting the krfCreated flag
    // so that we don't try to recreate the krf.
    if (recoverValuesSync) {
      return false;
    }

    FileInputStream fis;
    try {
      fis = new FileInputStream(f);
    } catch (FileNotFoundException ignore) {
      return false;
    }
    try {
      if (getParent().isOffline() && !getParent().FORCE_KRF_RECOVERY) {
        return false;
      }
      logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_RECOVERING_OPLOG_0_1_2,
          new Object[] {toString(), f.getAbsolutePath(), getParent().getName()}));
      this.recoverNewEntryId = DiskStoreImpl.INVALID_ID;
      this.recoverModEntryId = DiskStoreImpl.INVALID_ID;
      this.recoverModEntryIdHWM = DiskStoreImpl.INVALID_ID;
      long oplogKeyIdHWM = DiskStoreImpl.INVALID_ID;
      int krfEntryCount = 0;
      DataInputStream dis = new DataInputStream(new BufferedInputStream(fis, 1024 * 1024));
      final Version version = getProductVersionIfOld();
      final ByteArrayDataInput in = new ByteArrayDataInput();
      try {
        try {
          validateOpcode(dis, OPLOG_MAGIC_SEQ_ID);
          readOplogMagicSeqRecord(dis, f, OPLOG_TYPE.KRF);

          validateOpcode(dis, OPLOG_DISK_STORE_ID);
          readDiskStoreRecord(dis, f);
        } catch (DiskAccessException ignore) {
          // Failed to read the file. There are two possibilities. Either this
          // file is in old format which does not have a magic seq in the
          // beginning or this is not a valid file at all. Try reading it as a
          // file in old format
          fis.close();
          fis = new FileInputStream(f);
          dis = new DataInputStream(new BufferedInputStream(fis, 1024 * 1024));
          readDiskStoreRecord(dis, f);
        } catch (IllegalStateException ignore) {
          // Failed to read the file. There are two possibilities. Either this
          // is in new format which has a magic seq in the beginning or this is
          // not a valid file at all
          fis.close();
          fis = new FileInputStream(f);
          dis = new DataInputStream(new BufferedInputStream(fis, 1024 * 1024));
          readDiskStoreRecord(dis, f);
        }

        readGemfireVersionRecord(dis, f);
        readTotalCountRecord(dis, f);
        readRVVRecord(dis, f, false, latestOplog);
        long lastOffset = 0;
        byte[] keyBytes = DataSerializer.readByteArray(dis);
        while (keyBytes != null) {
          byte userBits = dis.readByte();
          int valueLength = InternalDataSerializer.readArrayLength(dis);
          byte[] valueBytes = null;
          long drId = DiskInitFile.readDiskRegionID(dis);
          DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);

          // read version
          VersionTag tag = null;
          if (EntryBits.isWithVersions(userBits)) {
            tag = readVersionsFromOplog(dis);
            if (drs != null && !drs.getDiskRegionView().getFlags()
                .contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
              // 50044 Remove version tag from entry if we don't want versioning
              // for this region
              tag = null;
              userBits = EntryBits.setWithVersions(userBits, false);
            } else {
              // Update the RVV with the new entry
              if (drs != null) {
                drs.recordRecoveredVersionTag(tag);
              }
            }
          }

          long oplogKeyId = InternalDataSerializer.readVLOld(dis);
          long oplogOffset;
          if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
            oplogOffset = -1;
          } else {
            oplogOffset = lastOffset + InternalDataSerializer.readVLOld(dis);
            lastOffset = oplogOffset;
          }

          if (oplogKeyId > oplogKeyIdHWM) {
            oplogKeyIdHWM = oplogKeyId;
          }
          if (okToSkipModifyRecord(deletedIds, drId, drs, oplogKeyId, true, tag).skip()) {
            if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "readNewEntry skipping oplogKeyId=<{}> drId={} userBits={} oplogOffset={} valueLen={}",
                  oplogKeyId, drId, userBits, oplogOffset, valueLength);
            }
            this.stats.incRecoveryRecordsSkipped();
            incSkipped();
          } else {
            if (EntryBits.isAnyInvalid(userBits)) {
              if (EntryBits.isInvalid(userBits)) {
                valueBytes = DiskEntry.INVALID_BYTES;
              } else {
                valueBytes = DiskEntry.LOCAL_INVALID_BYTES;
              }
            } else if (EntryBits.isTombstone(userBits)) {
              valueBytes = DiskEntry.TOMBSTONE_BYTES;
            }
            Object key = deserializeKey(keyBytes, version, in);
            {
              Object oldValue = getRecoveryMap().put(oplogKeyId, key);
              if (oldValue != null) {
                throw new AssertionError(
                    LocalizedStrings.Oplog_DUPLICATE_CREATE.toLocalizedString(oplogKeyId));
              }
            }
            DiskEntry de = drs.getDiskEntry(key);
            if (de == null) {
              if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
                logger.trace(LogMarker.PERSIST_RECOVERY,
                    "readNewEntry oplogKeyId=<{}> drId={} userBits={} oplogOffset={} valueLen={}",
                    oplogKeyId, drId, userBits, oplogOffset, valueLength);
              }
              DiskEntry.RecoveredEntry re = createRecoveredEntry(valueBytes, valueLength, userBits,
                  getOplogId(), oplogOffset, oplogKeyId, false, version, in);
              if (tag != null) {
                re.setVersionTag(tag);
              }
              initRecoveredEntry(drs.getDiskRegionView(), drs.initializeRecoveredEntry(key, re));
              drs.getDiskRegionView().incRecoveredEntryCount();
              this.stats.incRecoveredEntryCreates();
              krfEntryCount++;
            } else {
              DiskId curdid = de.getDiskId();
              // assert curdid.getOplogId() != getOplogId();
              if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
                logger.trace(LogMarker.PERSIST_RECOVERY,
                    "ignore readNewEntry because getOplogId()={} != curdid.getOplogId()={} for drId={} key={}",
                    getOplogId(), curdid.getOplogId(), drId, key);
              }
            }
          }
          keyBytes = DataSerializer.readByteArray(dis);
        } // while
        setRecoverNewEntryId(oplogKeyIdHWM);
      } catch (IOException ex) {
        try {
          fis.close();
          fis = null;
        } catch (IOException ignore) {
        }
        throw new DiskAccessException("Unable to recover from krf file for oplogId=" + oplogId
            + ", file=" + f.getName() + ". This file is corrupt, but may be safely deleted.", ex,
            getParent());
      }
      if (recoverValues && krfEntryCount > 0) {
        oplogsNeedingValueRecovery.add(this);
        // TODO optimize this code and make it async
        // It should also honor the lru limit
        // The fault in logic might not work until
        // the region is actually created.
        // Instead of reading the crf it might be better to iterate the live
        // entry
        // list that was built during KRF recovery. Just fault values in until
        // we
        // hit the LRU limit (if we have one). Only fault in values for entries
        // recovered from disk that are still in this oplog.
        // Defer faulting in values until all oplogs for the ds have been
        // recovered.
      }
    } finally {
      // fix for bug 42776
      if (fis != null) {
        try {
          fis.close();
          fis = null;
        } catch (IOException ignore) {
        }
      }
    }
    return true;
  }

  private void validateOpcode(DataInputStream dis, byte expect) throws IOException {
    byte opCode = dis.readByte();
    if (opCode != expect) {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "expected opcode id absent: {}", expect);
      }
      throw new IllegalStateException();
    }
  }

  /**
   * Return number of bytes read
   */
  private long readCrf(OplogEntryIdSet deletedIds, boolean recoverValues, boolean latestOplog) {
    this.recoverNewEntryId = DiskStoreImpl.INVALID_ID;
    this.recoverModEntryId = DiskStoreImpl.INVALID_ID;
    this.recoverModEntryIdHWM = DiskStoreImpl.INVALID_ID;
    boolean readLastRecord = true;
    CountingDataInputStream dis = null;
    try {
      final LocalRegion currentRegion = LocalRegion.getInitializingRegion();
      final Version version = getProductVersionIfOld();
      final ByteArrayDataInput in = new ByteArrayDataInput();
      final HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
      int recordCount = 0;
      boolean foundDiskStoreRecord = false;
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(this.crf.f);
        dis = new CountingDataInputStream(new BufferedInputStream(fis, 1024 * 1024),
            this.crf.f.length());
        boolean endOfLog = false;
        while (!endOfLog) {
          // long startPosition = byteCount;
          if (dis.atEndOfFile()) {
            endOfLog = true;
            break;
          }
          readLastRecord = false;
          byte opCode = dis.readByte();
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY, "Oplog opCode={}", opCode);
          }
          switch (opCode) {
            case OPLOG_EOF_ID:
              // we are at the end of the oplog. So we need to back up one byte
              dis.decrementCount();
              endOfLog = true;
              break;
            case OPLOG_CONFLICT_VERSION:
              this.readVersionTagOnlyEntry(dis, opCode);
              break;
            case OPLOG_NEW_ENTRY_BASE_ID: {
              long newEntryBase = dis.readLong();
              if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
                logger.trace(LogMarker.PERSIST_RECOVERY, "newEntryBase={}", newEntryBase);
              }
              readEndOfRecord(dis);
              setRecoverNewEntryId(newEntryBase);
              recordCount++;
            }
              break;
            case OPLOG_NEW_ENTRY_0ID:
              readNewEntry(dis, opCode, deletedIds, recoverValues, currentRegion, version, in,
                  hdos);
              recordCount++;
              break;
            case OPLOG_MOD_ENTRY_1ID:
            case OPLOG_MOD_ENTRY_2ID:
            case OPLOG_MOD_ENTRY_3ID:
            case OPLOG_MOD_ENTRY_4ID:
            case OPLOG_MOD_ENTRY_5ID:
            case OPLOG_MOD_ENTRY_6ID:
            case OPLOG_MOD_ENTRY_7ID:
            case OPLOG_MOD_ENTRY_8ID:
              readModifyEntry(dis, opCode, deletedIds, recoverValues, currentRegion, version, in,
                  hdos);
              recordCount++;
              break;
            case OPLOG_MOD_ENTRY_WITH_KEY_1ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_2ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_3ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_4ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_5ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_6ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_7ID:
            case OPLOG_MOD_ENTRY_WITH_KEY_8ID:
              readModifyEntryWithKey(dis, opCode, deletedIds, recoverValues, currentRegion, version,
                  in, hdos);
              recordCount++;
              break;

            case OPLOG_DISK_STORE_ID:
              readDiskStoreRecord(dis, this.crf.f);
              foundDiskStoreRecord = true;
              recordCount++;
              break;
            case OPLOG_MAGIC_SEQ_ID:
              readOplogMagicSeqRecord(dis, this.crf.f, OPLOG_TYPE.CRF);
              break;
            case OPLOG_GEMFIRE_VERSION:
              readGemfireVersionRecord(dis, this.crf.f);
              recordCount++;
              break;
            case OPLOG_RVV:
              readRVVRecord(dis, this.drf.f, false, latestOplog);
              recordCount++;
              break;
            default:
              throw new DiskAccessException(
                  LocalizedStrings.Oplog_UNKNOWN_OPCODE_0_FOUND_IN_DISK_OPERATION_LOG
                      .toLocalizedString(opCode),
                  getParent());
          }
          readLastRecord = true;
          // @todo
          // if (rgn.isDestroyed()) {
          // break;
          // }
        } // while
      } finally {
        if (dis != null) {
          dis.close();
        }
        if (fis != null) {
          fis.close();
        }
      }
      if (!foundDiskStoreRecord && recordCount > 0) {
        throw new DiskAccessException(
            "The oplog file \"" + this.crf.f + "\" does not belong to the init file \""
                + getParent().getInitFile() + "\". Crf did not contain a disk store id.",
            getParent());
      }
    } catch (EOFException ignore) {
      // ignore since a partial record write can be caused by a crash
    } catch (IOException ex) {
      getParent().getCancelCriterion().checkCancelInProgress(ex);
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_READING_FILE_DURING_RECOVERY_FROM_0
              .toLocalizedString(this.crf.f.getPath()),
          ex, getParent());
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Oplog::readOplog:Error in recovery as Cache was closed", e);
      }
    } catch (RegionDestroyedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Oplog::readOplog:Error in recovery as Region was destroyed", e);
      }
    } catch (IllegalStateException e) {
      throw e;
    }

    // Add the Oplog size to the Directory Holder which owns this oplog,
    // so that available space is correctly calculated & stats updated.
    long byteCount = 0;
    if (!readLastRecord) {
      // this means that there was a crash
      // and hence we should not continue to read
      // the next oplog
      this.crashed = true;
      if (dis != null) {
        byteCount = dis.getFileLength();
      }
    } else {
      if (dis != null) {
        byteCount = dis.getCount();
      }
    }
    return byteCount;
  }

  /**
   * @throws DiskAccessException if this file does not belong to our parent
   */
  private void readDiskStoreRecord(DataInput dis, File f) throws IOException {
    long leastSigBits = dis.readLong();
    long mostSigBits = dis.readLong();
    DiskStoreID readDSID = new DiskStoreID(mostSigBits, leastSigBits);
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY, "diskStoreId={}", readDSID);
    }
    readEndOfRecord(dis);
    DiskStoreID dsid = getParent().getDiskStoreID();
    if (!readDSID.equals(dsid)) {
      throw new DiskAccessException("The oplog file \"" + f
          + "\" does not belong to the init file \"" + getParent().getInitFile() + "\".",
          getParent());
    }
  }

  /*
   * Reads and validates magic sequence in oplog header. For existing files this will not exist.
   * This method will throw a DiskAccessException in that case too.
   */
  private void readOplogMagicSeqRecord(DataInput dis, File f, OPLOG_TYPE type) throws IOException {
    byte[] seq = new byte[OPLOG_TYPE.getLen()];
    dis.readFully(seq);
    for (int i = 0; i < OPLOG_TYPE.getLen(); i++) {
      if (seq[i] != type.getBytes()[i]) {
        if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
          logger.trace(LogMarker.PERSIST_RECOVERY,
              "oplog magic code mismatched at byte:{}, value:{}", (i + 1), seq[i]);
        }
        throw new DiskAccessException("Invalid oplog (" + type.name() + ") file provided: " + f,
            getParent());
      }
    }
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < OPLOG_TYPE.getLen(); i++) {
        sb.append(" ").append(seq[i]);
      }
      logger.trace(LogMarker.PERSIST_RECOVERY, "oplog magic code: {}", sb);
    }
    readEndOfRecord(dis);
  }

  /**
   * @throws DiskAccessException if this file does not belong to our parent
   */
  private void readGemfireVersionRecord(DataInput dis, File f) throws IOException {
    Version recoveredGFVersion = readProductVersionRecord(dis, f);
    final boolean hasDataVersion;
    if ((hasDataVersion = (recoveredGFVersion == Version.TOKEN))) {
      // actual GFE version will be the next record in this case
      byte opCode = dis.readByte();
      if (opCode != OPLOG_GEMFIRE_VERSION) {
        throw new DiskAccessException(
            LocalizedStrings.Oplog_UNKNOWN_OPCODE_0_FOUND_IN_DISK_OPERATION_LOG
                .toLocalizedString(opCode),
            getParent());
      }
      recoveredGFVersion = readProductVersionRecord(dis, f);
    }
    if (this.gfversion == null) {
      this.gfversion = recoveredGFVersion;
    } else {
      assert this.gfversion == recoveredGFVersion;
    }
    if (hasDataVersion) {
      byte opCode = dis.readByte();
      if (opCode != OPLOG_GEMFIRE_VERSION) {
        throw new DiskAccessException(
            LocalizedStrings.Oplog_UNKNOWN_OPCODE_0_FOUND_IN_DISK_OPERATION_LOG
                .toLocalizedString(opCode),
            getParent());
      }
      recoveredGFVersion = readProductVersionRecord(dis, f);
      if (this.dataVersion == null) {
        this.dataVersion = recoveredGFVersion;
      } else {
        assert this.dataVersion == recoveredGFVersion;
      }
    }
  }

  private Version readProductVersionRecord(DataInput dis, File f) throws IOException {
    Version recoveredGFVersion;
    short ver = Version.readOrdinal(dis);
    try {
      recoveredGFVersion = Version.fromOrdinal(ver, false);
    } catch (UnsupportedVersionException e) {
      throw new DiskAccessException(
          LocalizedStrings.Oplog_UNEXPECTED_PRODUCT_VERSION_0.toLocalizedString(ver), e,
          getParent());
    }
    logger.trace(LogMarker.PERSIST_RECOVERY, "version={}", recoveredGFVersion);
    readEndOfRecord(dis);
    return recoveredGFVersion;
  }

  private void readTotalCountRecord(DataInput dis, File f) throws IOException {
    long recoveredCount = InternalDataSerializer.readUnsignedVL(dis);
    this.totalCount.set(recoveredCount);

    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY, "totalCount={}", totalCount);
    }
    readEndOfRecord(dis);
  }

  private void readRVVRecord(DataInput dis, File f, boolean gcRVV, boolean latestOplog)
      throws IOException {
    final boolean isPersistRecoveryDebugEnabled = logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY);

    long numRegions = InternalDataSerializer.readUnsignedVL(dis);
    if (isPersistRecoveryDebugEnabled) {
      logger.trace(LogMarker.PERSIST_RECOVERY, "readRVV entry numRegions={}", numRegions);
    }
    for (int region = 0; region < numRegions; region++) {
      long drId = InternalDataSerializer.readUnsignedVL(dis);
      // Get the drs. This may be null if this region is not currently
      // recovering
      DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);
      if (isPersistRecoveryDebugEnabled) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "readRVV drId={} region={}", drId, drs);
      }

      if (gcRVV) {

        // Read the GCC RV
        long rvvSize = InternalDataSerializer.readUnsignedVL(dis);
        for (int memberNum = 0; memberNum < rvvSize; memberNum++) {
          // for each member, read the member id and version
          long memberId = InternalDataSerializer.readUnsignedVL(dis);
          long gcVersion = InternalDataSerializer.readUnsignedVL(dis);

          // if we have a recovery store, add the recovered regions
          if (drs != null) {
            Object member = getParent().getDiskInitFile().getCanonicalObject((int) memberId);
            drs.recordRecoveredGCVersion((VersionSource) member, gcVersion);
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "adding gcRVV entry drId={}, member={}, version={}", drId, memberId, gcVersion);
            }
          } else {
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "skipping gcRVV entry drId={}, member={}, version={}", drId, memberId, gcVersion);
            }
          }
        }
      } else {
        boolean rvvTrusted = DataSerializer.readBoolean(dis);
        if (drs != null) {
          if (latestOplog) {
            // only set rvvtrust based on the newest oplog recovered
            drs.setRVVTrusted(rvvTrusted);
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY, "marking RVV trusted drId={},tvvTrusted={}",
                  drId, rvvTrusted);
            }
          }
        }
        // Read a regular RVV
        long rvvSize = InternalDataSerializer.readUnsignedVL(dis);
        for (int memberNum = 0; memberNum < rvvSize; memberNum++) {

          // for each member, read the member id and version
          long memberId = InternalDataSerializer.readUnsignedVL(dis);
          RegionVersionHolder versionHolder = new RegionVersionHolder(dis);
          if (drs != null) {
            Object member = getParent().getDiskInitFile().getCanonicalObject((int) memberId);
            drs.recordRecoveredVersonHolder((VersionSource) member, versionHolder, latestOplog);
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "adding RVV entry drId={},member={},versionHolder={},latestOplog={},oplogId={}",
                  drId, memberId, versionHolder, latestOplog, getOplogId());
            }
          } else {
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "skipping RVV entry drId={}, member={}, versionHolder={}", drId, memberId,
                  versionHolder);
            }
          }
        }
      }
    }
    readEndOfRecord(dis);
  }

  /**
   * Recovers one oplog
   * 
   * @param latestOplog - true if this oplog is the latest oplog in the disk store.
   */
  long recoverCrf(OplogEntryIdSet deletedIds, boolean recoverValues, boolean recoverValuesSync,
      boolean alreadyRecoveredOnce, Set<Oplog> oplogsNeedingValueRecovery, boolean latestOplog) {
    // crf might not exist; but drf always will
    this.diskFile = new File(this.drf.f.getParentFile(),
        oplogSet.getPrefix() + getParent().getName() + "_" + this.oplogId);

    File crfFile = this.crf.f;
    if (crfFile == null) {
      this.haveRecoveredCrf = true;
      return 0L;
    }

    lockCompactor();
    this.kvMap = new OplogEntryIdMap();
    this.skippedKeyBytes = new OplogEntryIdMap();
    try {
      if (this.haveRecoveredCrf && isDeleted())
        return 0; // do this check while holding lock
      if (!this.haveRecoveredCrf) {
        this.haveRecoveredCrf = true;
      }

      long byteCount;
      // if we have a KRF then read it and delay reading the CRF.
      // Unless we are in synchronous recovery mode
      if (!readKrf(deletedIds, recoverValues, recoverValuesSync, oplogsNeedingValueRecovery,
          latestOplog)) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_RECOVERING_OPLOG_0_1_2,
            new Object[] {toString(), crfFile.getAbsolutePath(), getParent().getName()}));
        byteCount = readCrf(deletedIds, recoverValues, latestOplog);
      } else {
        byteCount = this.crf.f.length();
      }
      if (!isPhase2()) {
        if (getParent().isOfflineCompacting()) {
          getParent().incLiveEntryCount(getRecoveryMap().size());
        }
        getParent().incDeadRecordCount(getRecordsSkipped());
      }
      if (getParent().isOfflineCompacting()) {
        offlineCompact(deletedIds, latestOplog);
      }
      if (!alreadyRecoveredOnce) {
        setRecoveredCrfSize(byteCount);
        this.dirHolder.incrementTotalOplogSize(byteCount);
      }
      if (getParent().isOfflineCompacting()) {
        if (isOplogEmpty()) {
          this.deleted.set(false);
          destroy();
        }
      }
      return byteCount;
    } finally {
      this.kvMap = null;
      this.skippedKeyBytes = null;
      unlockCompactor();
    }
  }

  private boolean offlineCompactPhase2 = false;

  private boolean isPhase1() {
    return !this.offlineCompactPhase2;
  }

  private boolean isPhase2() {
    return this.offlineCompactPhase2;
  }

  private void offlineCompact(OplogEntryIdSet deletedIds, boolean latestOplog) {
    // If we only do this if "(getRecordsSkipped() > 0)" then it will only
    // compact
    // an oplog that has some garbage in it.
    // Instead if we do every oplog in case they set maxOplogSize
    // then all oplogs will be converted to obey maxOplogSize.
    // 45777: for normal offline compaction, we only do it when
    // getRecordsSkipped() > 0
    // but for upgrade disk store, we have to do it for pure creates oplog
    if (getRecordsSkipped() > 0 || getHasDeletes() || getParent().isUpgradeVersionOnly()) {
      this.offlineCompactPhase2 = true;
      if (getOplogSet().getChild() == null) {
        getOplogSet().initChild();
      }
      readCrf(deletedIds, true, latestOplog);
      this.deleted.set(false);
      destroyCrfOnly();
    } else {
      // For every live entry in this oplog add it to the deleted set
      // so that we will skip it when we recovery the next oplogs.
      for (OplogEntryIdMap.Iterator it = getRecoveryMap().iterator(); it.hasNext();) {
        it.advance();
        deletedIds.add(it.key());
      }
      close();
    }
  }

  /**
   * TODO soplog - This method is public just to test soplog recovery
   */
  public DiskEntry.RecoveredEntry createRecoveredEntry(byte[] valueBytes, int valueLength,
      byte userBits, long oplogId, long offsetInOplog, long oplogKeyId, boolean recoverValue,
      Version version, ByteArrayDataInput in) {
    DiskEntry.RecoveredEntry re = null;
    if (recoverValue || EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
      Object value;
      if (EntryBits.isLocalInvalid(userBits)) {
        value = Token.LOCAL_INVALID;
        valueLength = 0;
      } else if (EntryBits.isInvalid(userBits)) {
        value = Token.INVALID;
        valueLength = 0;
      } else if (EntryBits.isSerialized(userBits)) {
        value = DiskEntry.Helper.readSerializedValue(valueBytes, version, in, false);
      } else if (EntryBits.isTombstone(userBits)) {
        value = Token.TOMBSTONE;
      } else {
        value = valueBytes;
      }
      re = new DiskEntry.RecoveredEntry(oplogKeyId, oplogId, offsetInOplog, userBits, valueLength,
          value);
    } else {
      re = new DiskEntry.RecoveredEntry(oplogKeyId, oplogId, offsetInOplog, userBits, valueLength);
    }
    return re;
  }

  private void readEndOfRecord(DataInput di) throws IOException {
    int b = di.readByte();
    if (b != END_OF_RECORD_ID) {
      if (b == 0) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.Oplog_PARTIAL_RECORD));

        // this is expected if this is the last record and we died while writing
        // it.
        throw new EOFException("found partial last record");
      } else {
        // Our implementation currently relies on all unwritten bytes having
        // a value of 0. So throw this exception if we find one we didn't
        // expect.
        throw new IllegalStateException(
            "expected end of record (byte==" + END_OF_RECORD_ID + ") or zero but found " + b);
      }
    }
  }

  private static void forceSkipBytes(CountingDataInputStream dis, int len) throws IOException {
    int skipped = dis.skipBytes(len);
    while (skipped < len) {
      dis.readByte();
      skipped++;
    }
  }

  private int recordsSkippedDuringRecovery = 0;

  private void incSkipped() {
    this.recordsSkippedDuringRecovery++;
  }

  int getRecordsSkipped() {
    return this.recordsSkippedDuringRecovery;
  }

  private VersionTag readVersionsFromOplog(DataInput dis) throws IOException {
    if (Version.GFE_70.compareTo(currentRecoveredGFVersion()) <= 0) {
      // this version format is for gemfire 7.0
      // if we have different version format in 7.1, it will be handled in
      // "else if"
      int entryVersion = (int) InternalDataSerializer.readSignedVL(dis);
      long regionVersion = InternalDataSerializer.readUnsignedVL(dis);
      int memberId = (int) InternalDataSerializer.readUnsignedVL(dis);
      Object member = getParent().getDiskInitFile().getCanonicalObject(memberId);
      long timestamp = InternalDataSerializer.readUnsignedVL(dis);
      int dsId = (int) InternalDataSerializer.readSignedVL(dis);
      VersionTag vt = VersionTag.create((VersionSource) member);
      vt.setEntryVersion(entryVersion);
      vt.setRegionVersion(regionVersion);
      vt.setMemberID((VersionSource) member);
      vt.setVersionTimeStamp(timestamp);
      vt.setDistributedSystemId(dsId);
      return vt;
    } else {
      // pre-7.0
      return null;
    }
  }

  private synchronized VersionTag createDummyTag(DiskRecoveryStore drs) {
    DiskStoreID member = getParent().getDiskStoreID();
    int memberid = getParent().getDiskInitFile().getOrCreateCanonicalId(member);
    long regionVersion = drs.getVersionForMember(member);
    VersionTag vt = VersionTag.create(member);
    vt.setEntryVersion(1);
    vt.setRegionVersion(regionVersion + 1);
    vt.setMemberID(member);
    vt.setVersionTimeStamp(getParent().getCache().cacheTimeMillis());
    vt.setDistributedSystemId(-1);
    return vt;
  }

  /**
   * Returns true if the values for the given disk recovery store should be recovered.
   */
  private boolean recoverLruValue(DiskRecoveryStore drs) {
    if (isLruValueRecoveryDisabled(drs)) {
      return false;
    } else if (drs.lruLimitExceeded()) {
      this.stats.incRecoveredValuesSkippedDueToLRU();
      return false;
    }
    return true;
  }

  private boolean isLruValueRecoveryDisabled(DiskRecoveryStore store) {
    return !store.getDiskStore().isOffline() && !getParent().RECOVER_LRU_VALUES
        && !store.getEvictionAttributes().getAlgorithm().isNone();
  }

  /**
   * Reads an oplog entry of type Create
   * 
   * @param dis DataInputStream from which the oplog is being read
   * @param opcode byte whether the id is short/int/long
   */
  private void readNewEntry(CountingDataInputStream dis, byte opcode, OplogEntryIdSet deletedIds,
      boolean recoverValue, final LocalRegion currentRegion, Version version, ByteArrayDataInput in,
      HeapDataOutputStream hdos) throws IOException {
    final boolean isPersistRecoveryDebugEnabled = logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY);

    long oplogOffset = -1;
    byte userBits = dis.readByte();
    byte[] objValue = null;
    int valueLength = 0;
    long oplogKeyId = incRecoverNewEntryId();
    long drId = DiskInitFile.readDiskRegionID(dis);
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);
    // read versions
    VersionTag tag = null;
    boolean isDummy = false;
    if (EntryBits.isWithVersions(userBits)) {
      tag = readVersionsFromOplog(dis);
      isDummy = false;
    } else if (getParent().isUpgradeVersionOnly() && drs != null) {
      tag = this.createDummyTag(drs);
      userBits = EntryBits.setWithVersions(userBits, true);
      isDummy = true;
    }
    if (drs != null
        && !drs.getDiskRegionView().getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
      // 50044 Remove version tag from entry if we don't want versioning for
      // this region
      tag = null;
      userBits = EntryBits.setWithVersions(userBits, false);
    }

    OkToSkipResult skipResult = okToSkipModifyRecord(deletedIds, drId, drs, oplogKeyId, true, tag);
    if (skipResult.skip()) {
      if (!isPhase2()) {
        this.stats.incRecoveryRecordsSkipped();
        incSkipped();
      }
    } else if (recoverValue && !getParent().isOfflineCompacting()) {
      recoverValue = recoverLruValue(drs);
    }

    CompactionRecord p2cr = null;
    long crOffset;
    if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
      if (EntryBits.isInvalid(userBits)) {
        objValue = DiskEntry.INVALID_BYTES;
      } else if (EntryBits.isTombstone(userBits)) {
        objValue = DiskEntry.TOMBSTONE_BYTES;
      } else {
        objValue = DiskEntry.LOCAL_INVALID_BYTES;
      }
      crOffset = dis.getCount();
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
    } else {
      int len = dis.readInt();
      oplogOffset = dis.getCount();
      crOffset = oplogOffset;
      valueLength = len;
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
      if (recoverValue && !skipResult.skip()) {
        byte[] valueBytes = new byte[len];
        dis.readFully(valueBytes);
        objValue = valueBytes;
        validateValue(valueBytes, userBits, version, in);
      } else {
        forceSkipBytes(dis, len);
      }
    }
    {
      int len = dis.readInt();
      incTotalCount();
      if (skipResult.skip()) {
        if (skipResult.skipKey()) {
          forceSkipBytes(dis, len);
        } else {
          byte[] keyBytes = new byte[len];
          dis.readFully(keyBytes);
          skippedKeyBytes.put(oplogKeyId, keyBytes);
        }
        readEndOfRecord(dis);

        if (drs != null && tag != null) {
          // Update the RVV with the new entry
          // This must be done after reading the end of record to make sure
          // we don't have a corrupt record. See bug #45538
          drs.recordRecoveredVersionTag(tag);
        }

        if (isPersistRecoveryDebugEnabled) {
          logger.trace(LogMarker.PERSIST_RECOVERY,
              "readNewEntry SKIPPING oplogKeyId=<{}> drId={} userBits={} keyLen={} valueLen={} tag={}",
              oplogKeyId, drId, userBits, len, valueLength, tag);
        }
      } else {
        byte[] keyBytes = null;
        if (isPhase2()) {
          forceSkipBytes(dis, len);
        } else {
          keyBytes = new byte[len];
          dis.readFully(keyBytes);
        }
        readEndOfRecord(dis);

        if (drs != null && tag != null) {
          // Update the RVV with the new entry
          // This must be done after reading the end of record to make sure
          // we don't have a corrupt record. See bug #45538
          drs.recordRecoveredVersionTag(tag);
        }
        if (getParent().isOfflineCompacting()) {
          if (isPhase1()) {
            CompactionRecord cr = new CompactionRecord(keyBytes, crOffset);
            getRecoveryMap().put(oplogKeyId, cr);
            drs.getDiskRegionView().incRecoveredEntryCount();
            this.stats.incRecoveredEntryCreates();
          } else { // phase2
            Assert.assertTrue(p2cr != null, "First pass did not find create a compaction record");
            getOplogSet().getChild().copyForwardForOfflineCompact(oplogKeyId, p2cr.getKeyBytes(),
                objValue, userBits, drId, tag);
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY, "readNewEntry copyForward oplogKeyId=<{}>",
                  oplogKeyId);
            }
            // add it to the deletedIds set so we will ignore it in earlier
            // oplogs
            deletedIds.add(oplogKeyId);
          }
        } else {
          Object key = deserializeKey(keyBytes, version, in);
          {
            Object oldValue = getRecoveryMap().put(oplogKeyId, key);
            if (oldValue != null) {
              throw new AssertionError(
                  LocalizedStrings.Oplog_DUPLICATE_CREATE.toLocalizedString(oplogKeyId));
            }
          }
          DiskEntry de = drs.getDiskEntry(key);
          if (de == null) {
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "readNewEntry oplogKeyId=<{}> drId={} key={} userBits={} oplogOffset={} valueLen={} tag={}",
                  oplogKeyId, drId, key, userBits, oplogOffset, valueLength, tag);
            }
            DiskEntry.RecoveredEntry re = createRecoveredEntry(objValue, valueLength, userBits,
                getOplogId(), oplogOffset, oplogKeyId, recoverValue, version, in);
            if (tag != null) {
              re.setVersionTag(tag);
            }
            initRecoveredEntry(drs.getDiskRegionView(), drs.initializeRecoveredEntry(key, re));
            drs.getDiskRegionView().incRecoveredEntryCount();
            this.stats.incRecoveredEntryCreates();

          } else {
            DiskId curdid = de.getDiskId();
            assert curdid.getOplogId() != getOplogId();
            if (isPersistRecoveryDebugEnabled) {
              logger.trace(LogMarker.PERSIST_RECOVERY,
                  "ignore readNewEntry because getOplogId()={} != curdid.getOplogId()={} for drId={} key={}",
                  getOplogId(), curdid.getOplogId(), drId, key);
            }
          }
        }
      }
    }
  }

  /**
   * Reads an oplog entry of type Modify
   * 
   * @param dis DataInputStream from which the oplog is being read
   * @param opcode byte whether the id is short/int/long
   */
  private void readModifyEntry(CountingDataInputStream dis, byte opcode, OplogEntryIdSet deletedIds,
      boolean recoverValue, LocalRegion currentRegion, Version version, ByteArrayDataInput in,
      HeapDataOutputStream hdos) throws IOException {
    final boolean isPersistRecoveryDebugEnabled = logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY);

    long oplogOffset = -1;
    byte userBits = dis.readByte();

    int idByteCount = (opcode - OPLOG_MOD_ENTRY_1ID) + 1;
    // long debugRecoverModEntryId = this.recoverModEntryId;
    long oplogKeyId = getModEntryId(dis, idByteCount);
    // long debugOplogKeyId = dis.readLong();
    // //assert oplogKeyId == debugOplogKeyId
    // // : "expected=" + debugOplogKeyId + " actual=" + oplogKeyId
    // assert debugRecoverModEntryId == debugOplogKeyId
    // : "expected=" + debugOplogKeyId + " actual=" + debugRecoverModEntryId
    // + " idByteCount=" + idByteCount
    // + " delta=" + this.lastDelta;
    long drId = DiskInitFile.readDiskRegionID(dis);
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);
    // read versions
    VersionTag tag = null;
    if (EntryBits.isWithVersions(userBits)) {
      tag = readVersionsFromOplog(dis);
    } else if (getParent().isUpgradeVersionOnly() && drs != null) {
      tag = this.createDummyTag(drs);
      userBits = EntryBits.setWithVersions(userBits, true);
    }
    if (drs != null
        && !drs.getDiskRegionView().getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
      // 50044 Remove version tag from entry if we don't want versioning for
      // this region
      tag = null;
      userBits = EntryBits.setWithVersions(userBits, false);
    }
    OkToSkipResult skipResult = okToSkipModifyRecord(deletedIds, drId, drs, oplogKeyId, false, tag);
    if (skipResult.skip()) {
      if (!isPhase2()) {
        incSkipped();
        this.stats.incRecoveryRecordsSkipped();
      }
    } else if (recoverValue && !getParent().isOfflineCompacting()) {
      recoverValue = recoverLruValue(drs);
    }

    byte[] objValue = null;
    int valueLength = 0;
    CompactionRecord p2cr = null;
    long crOffset;
    if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
      if (EntryBits.isInvalid(userBits)) {
        objValue = DiskEntry.INVALID_BYTES;
      } else if (EntryBits.isTombstone(userBits)) {
        objValue = DiskEntry.TOMBSTONE_BYTES;
      } else {
        objValue = DiskEntry.LOCAL_INVALID_BYTES;
      }
      crOffset = dis.getCount();
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
    } else {
      int len = dis.readInt();
      oplogOffset = dis.getCount();
      crOffset = oplogOffset;
      valueLength = len;
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
      if (!skipResult.skip() && recoverValue) {
        byte[] valueBytes = new byte[len];
        dis.readFully(valueBytes);
        objValue = valueBytes;
        validateValue(valueBytes, userBits, version, in);
      } else {
        forceSkipBytes(dis, len);
      }
    }
    readEndOfRecord(dis);

    if (drs != null && tag != null) {
      // Update the RVV with the new entry
      // This must be done after reading the end of record to make sure
      // we don't have a corrupt record. See bug #45538
      drs.recordRecoveredVersionTag(tag);
    }

    incTotalCount();
    if (!skipResult.skip()) {
      Object key = getRecoveryMap().get(oplogKeyId);

      // if the key is not in the recover map, it's possible it
      // was previously skipped. Check the skipped bytes map for the key.
      if (key == null) {
        byte[] keyBytes = (byte[]) skippedKeyBytes.get(oplogKeyId);
        if (keyBytes != null) {
          key = deserializeKey(keyBytes, version, in);
        }
      }
      if (isPersistRecoveryDebugEnabled) {
        logger.trace(LogMarker.PERSIST_RECOVERY,
            "readModifyEntry oplogKeyId=<{}> drId={} key=<{}> userBits={} oplogOffset={} tag={} valueLen={}",
            oplogKeyId, drId, key, userBits, oplogOffset, tag, valueLength);
      }
      // Will no longer be null since 1st modify record in any oplog
      // will now be a MOD_ENTRY_WITH_KEY record.
      assert key != null;

      if (getParent().isOfflineCompacting()) {
        if (isPhase1()) {
          CompactionRecord cr = (CompactionRecord) key;
          incSkipped(); // we are going to compact the previous record away
          cr.update(crOffset);
        } else { // phase2
          Assert.assertTrue(p2cr != null, "First pass did not find create a compaction record");
          getOplogSet().getChild().copyForwardForOfflineCompact(oplogKeyId, p2cr.getKeyBytes(),
              objValue, userBits, drId, tag);
          if (isPersistRecoveryDebugEnabled) {
            logger.trace(LogMarker.PERSIST_RECOVERY, "readModifyEntry copyForward oplogKeyId=<{}>",
                oplogKeyId);
          }
          // add it to the deletedIds set so we will ignore it in earlier oplogs
          deletedIds.add(oplogKeyId);
        }
      } else {
        // Check the actual region to see if it has this key from
        // a previous recovered oplog.
        DiskEntry de = drs.getDiskEntry(key);

        // This may actually be create, if the previous create or modify
        // of this entry was cleared through the RVV clear.
        if (de == null) {
          DiskRegionView drv = drs.getDiskRegionView();
          // and create an entry
          DiskEntry.RecoveredEntry re = createRecoveredEntry(objValue, valueLength, userBits,
              getOplogId(), oplogOffset, oplogKeyId, recoverValue, version, in);
          if (tag != null) {
            re.setVersionTag(tag);
          }
          if (isPersistRecoveryDebugEnabled) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "readModEntryWK init oplogKeyId=<{}> drId={} key=<{}> oplogOffset={} userBits={} valueLen={} tag={}",
                oplogKeyId, drId, key, oplogOffset, userBits, valueLength, tag);
          }
          initRecoveredEntry(drv, drs.initializeRecoveredEntry(key, re));
          drs.getDiskRegionView().incRecoveredEntryCount();
          this.stats.incRecoveredEntryCreates();

        } else {
          DiskEntry.RecoveredEntry re = createRecoveredEntry(objValue, valueLength, userBits,
              getOplogId(), oplogOffset, oplogKeyId, recoverValue, version, in);
          if (tag != null) {
            re.setVersionTag(tag);
          }
          de = drs.updateRecoveredEntry(key, re);
          updateRecoveredEntry(drs.getDiskRegionView(), de, re);

          this.stats.incRecoveredEntryUpdates();

        }
      }
    } else {
      if (isPersistRecoveryDebugEnabled) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "skipping readModifyEntry oplogKeyId=<{}> drId={}",
            oplogKeyId, drId);
      }
    }
  }

  private void readVersionTagOnlyEntry(CountingDataInputStream dis, byte opcode)
      throws IOException {
    long oplogOffset = -1;
    long drId = DiskInitFile.readDiskRegionID(dis);
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);
    // read versions
    VersionTag tag = readVersionsFromOplog(dis);
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY, "readVersionTagOnlyEntry drId={} tag={}", drId, tag);
    }
    readEndOfRecord(dis);

    // Update the RVV with the new entry
    if (drs != null) {
      drs.recordRecoveredVersionTag(tag);
    }
  }

  private void validateValue(byte[] valueBytes, byte userBits, Version version,
      ByteArrayDataInput in) {
    if (getParent().isValidating()) {
      if (EntryBits.isSerialized(userBits)) {
        // make sure values are deserializable
        if (!PdxWriterImpl.isPdx(valueBytes)) { // fix bug 43011
          try {
            DiskEntry.Helper.readSerializedValue(valueBytes, version, in, true);
          } catch (SerializationException ex) {
            if (logger.isDebugEnabled()) {
              logger.debug("Could not deserialize recovered value: {}", ex.getCause(), ex);
            }
          }
        }
      }
    }
  }

  /**
   * Reads an oplog entry of type ModifyWithKey
   * 
   * @param dis DataInputStream from which the oplog is being read
   * @param opcode byte whether the id is short/int/long
   */
  private void readModifyEntryWithKey(CountingDataInputStream dis, byte opcode,
      OplogEntryIdSet deletedIds, boolean recoverValue, final LocalRegion currentRegion,
      Version version, ByteArrayDataInput in, HeapDataOutputStream hdos) throws IOException {
    long oplogOffset = -1;

    byte userBits = dis.readByte();

    int idByteCount = (opcode - OPLOG_MOD_ENTRY_WITH_KEY_1ID) + 1;
    // long debugRecoverModEntryId = this.recoverModEntryId;
    long oplogKeyId = getModEntryId(dis, idByteCount);
    // long debugOplogKeyId = dis.readLong();
    // //assert oplogKeyId == debugOplogKeyId
    // // : "expected=" + debugOplogKeyId + " actual=" + oplogKeyId
    // assert debugRecoverModEntryId == debugOplogKeyId
    // : "expected=" + debugOplogKeyId + " actual=" + debugRecoverModEntryId
    // + " idByteCount=" + idByteCount
    // + " delta=" + this.lastDelta;
    long drId = DiskInitFile.readDiskRegionID(dis);
    DiskRecoveryStore drs = getOplogSet().getCurrentlyRecovering(drId);

    // read version
    VersionTag tag = null;
    if (EntryBits.isWithVersions(userBits)) {
      tag = readVersionsFromOplog(dis);
    } else if (getParent().isUpgradeVersionOnly() && drs != null) {
      tag = this.createDummyTag(drs);
      userBits = EntryBits.setWithVersions(userBits, true);
    }
    if (drs != null
        && !drs.getDiskRegionView().getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
      // 50044 Remove version tag from entry if we don't want versioning for
      // this region
      tag = null;
      userBits = EntryBits.setWithVersions(userBits, false);
    }
    OkToSkipResult skipResult = okToSkipModifyRecord(deletedIds, drId, drs, oplogKeyId, true, tag);
    if (skipResult.skip()) {
      if (!isPhase2()) {
        incSkipped();
        this.stats.incRecoveryRecordsSkipped();
      }
    } else if (recoverValue && !getParent().isOfflineCompacting()) {
      recoverValue = recoverLruValue(drs);
    }

    byte[] objValue = null;
    int valueLength = 0;
    CompactionRecord p2cr = null;
    long crOffset;
    if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
      if (EntryBits.isInvalid(userBits)) {
        objValue = DiskEntry.INVALID_BYTES;
      } else if (EntryBits.isTombstone(userBits)) {
        objValue = DiskEntry.TOMBSTONE_BYTES;
      } else {
        objValue = DiskEntry.LOCAL_INVALID_BYTES;
      }
      crOffset = dis.getCount();
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
    } else {
      int len = dis.readInt();
      oplogOffset = dis.getCount();
      crOffset = oplogOffset;
      valueLength = len;
      if (!skipResult.skip()) {
        if (isPhase2()) {
          p2cr = (CompactionRecord) getRecoveryMap().get(oplogKeyId);
          if (p2cr != null && p2cr.getOffset() != crOffset) {
            skipResult = OkToSkipResult.SKIP_RECORD;
          }
        }
      }
      if (!skipResult.skip() && recoverValue) {
        byte[] valueBytes = new byte[len];
        dis.readFully(valueBytes);
        objValue = valueBytes;
        validateValue(valueBytes, userBits, version, in);
      } else {
        forceSkipBytes(dis, len);
      }
    }

    int keyLen = dis.readInt();

    incTotalCount();
    if (skipResult.skip()) {
      if (skipResult.skipKey()) {
        forceSkipBytes(dis, keyLen);
      } else {
        byte[] keyBytes = new byte[keyLen];
        dis.readFully(keyBytes);
        skippedKeyBytes.put(oplogKeyId, keyBytes);
      }
      readEndOfRecord(dis);
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY,
            "skipping readModEntryWK init oplogKeyId=<{}> drId={}", oplogKeyId, drId);
      }
    } else {
      // read the key
      byte[] keyBytes = null;
      if (isPhase2()) {
        forceSkipBytes(dis, keyLen);
      } else {
        keyBytes = new byte[keyLen];
        dis.readFully(keyBytes);
      }
      readEndOfRecord(dis);
      if (drs != null && tag != null) {
        // Update the RVV with the new entry
        // This must be done after reading the end of record to make sure
        // we don't have a corrupt record. See bug #45538
        drs.recordRecoveredVersionTag(tag);
      }
      assert oplogKeyId >= 0;
      if (getParent().isOfflineCompacting()) {
        if (isPhase1()) {
          CompactionRecord cr = new CompactionRecord(keyBytes, crOffset);
          getRecoveryMap().put(oplogKeyId, cr);
          drs.getDiskRegionView().incRecoveredEntryCount();
          this.stats.incRecoveredEntryCreates();
        } else { // phase2
          Assert.assertTrue(p2cr != null, "First pass did not find create a compaction record");
          getOplogSet().getChild().copyForwardForOfflineCompact(oplogKeyId, p2cr.getKeyBytes(),
              objValue, userBits, drId, tag);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "readModifyEntryWithKey copyForward oplogKeyId=<{}>", oplogKeyId);
          }
          // add it to the deletedIds set so we will ignore it in earlier oplogs
          deletedIds.add(oplogKeyId);
        }
      } else {
        Object key = deserializeKey(keyBytes, version, in);
        Object oldValue = getRecoveryMap().put(oplogKeyId, key);
        if (oldValue != null) {
          throw new AssertionError(
              LocalizedStrings.Oplog_DUPLICATE_CREATE.toLocalizedString(oplogKeyId));
        }
        // Check the actual region to see if it has this key from
        // a previous recovered oplog.
        DiskEntry de = drs.getDiskEntry(key);
        if (de == null) {
          DiskRegionView drv = drs.getDiskRegionView();
          // and create an entry
          DiskEntry.RecoveredEntry re = createRecoveredEntry(objValue, valueLength, userBits,
              getOplogId(), oplogOffset, oplogKeyId, recoverValue, version, in);
          if (tag != null) {
            re.setVersionTag(tag);
          }
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "readModEntryWK init oplogKeyId=<{}> drId={} key={} oplogOffset={} userBits={} valueLen={} tag={}",
                oplogKeyId, drId, key, oplogOffset, userBits, valueLength, tag);
          }
          initRecoveredEntry(drv, drs.initializeRecoveredEntry(key, re));
          drs.getDiskRegionView().incRecoveredEntryCount();
          this.stats.incRecoveredEntryCreates();

        } else {
          DiskId curdid = de.getDiskId();
          assert curdid
              .getOplogId() != getOplogId() : "Mutiple ModEntryWK in the same oplog for getOplogId()="
                  + getOplogId() + " , curdid.getOplogId()=" + curdid.getOplogId() + " , for drId="
                  + drId + " , key=" + key;
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "ignore readModEntryWK because getOplogId()={} != curdid.getOplogId()={} for drId={} key={}",
                getOplogId(), curdid.getOplogId(), drId, key);
          }
          // de = drs.updateRecoveredEntry(key, re);
          // updateRecoveredEntry(drv, de, re);
          // this.stats.incRecoveredEntryUpdates();
        }
      }
    }
  }

  /**
   * Reads an oplog entry of type Delete
   * 
   * @param dis DataInputStream from which the oplog is being read
   * @param opcode byte whether the id is short/int/long
   * @param parent instance of disk region
   */
  private void readDelEntry(CountingDataInputStream dis, byte opcode, OplogEntryIdSet deletedIds,
      DiskStoreImpl parent) throws IOException {
    int idByteCount = (opcode - OPLOG_DEL_ENTRY_1ID) + 1;
    // long debugRecoverDelEntryId = this.recoverDelEntryId;
    long oplogKeyId = getDelEntryId(dis, idByteCount);
    // long debugOplogKeyId = dis.readLong();
    readEndOfRecord(dis);
    // assert debugRecoverDelEntryId == debugOplogKeyId
    // : "expected=" + debugOplogKeyId + " actual=" + debugRecoverDelEntryId
    // + " idByteCount=" + idByteCount
    // + " delta=" + this.lastDelta;
    deletedIds.add(oplogKeyId);
    setHasDeletes(true);
    this.stats.incRecoveredEntryDestroys();
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
      logger.trace(LogMarker.PERSIST_RECOVERY, "readDelEntry oplogKeyId=<{}>", oplogKeyId);
    }
  }

  /**
   * Keeps track of the drId of Regions that have records in this oplog that have not yet been
   * recovered. If this count is > 0 then this oplog can not be compacted.
   */
  private final AtomicInteger unrecoveredRegionCount = new AtomicInteger();

  private void addUnrecoveredRegion(long drId) {
    DiskRegionInfo dri = getOrCreateDRI(drId);
    if (dri.testAndSetUnrecovered()) {
      this.unrecoveredRegionCount.incrementAndGet();
    }
  }

  /**
   * For each dri that this oplog has that is currently unrecoverable check to see if a DiskRegion
   * that is recoverable now exists.
   */
  void checkForRecoverableRegion(DiskRegionView dr) {
    if (this.unrecoveredRegionCount.get() > 0) {
      DiskRegionInfo dri = getDRI(dr);
      if (dri != null) {
        if (dri.testAndSetRecovered(dr)) {
          this.unrecoveredRegionCount.decrementAndGet();
        }
      }
    }
  }

  void updateDiskRegion(DiskRegionView dr) {
    DiskRegionInfo dri = getDRI(dr);
    if (dri != null) {
      dri.setDiskRegion(dr);
    }
  }

  /**
   * Returns true if it is ok the skip the current modify record which had the given oplogEntryId.
   * It is ok to skip if any of the following are true: 1. deletedIds contains the id 2. the last
   * modification of the entry was done by a record read from an oplog other than this oplog
   */
  private OkToSkipResult okToSkipModifyRecord(OplogEntryIdSet deletedIds, long drId,
      DiskRecoveryStore drs, long oplogEntryId, boolean checkRecoveryMap, VersionTag tag) {
    if (deletedIds.contains(oplogEntryId)) {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY,
            "okToSkip because oplogEntryId={} was deleted for drId={}", oplogEntryId, drId);
      }
      return OkToSkipResult.SKIP_RECORD;
    }
    // if (dr == null || !dr.isReadyForRecovery()) {
    // // Region has not yet been created (it is not in the diskStore drMap).
    // // or it is not ready for recovery (i.e. it is a ProxyBucketRegion).
    // if (getParent().getDiskInitFile().regionExists(drId)
    // || (dr != null && !dr.isReadyForRecovery())) {
    // // Prevent compactor from removing this oplog.
    // // It needs to be in this state until all the regions stored it in
    // // are recovered.
    // addUnrecoveredRegion(drId);
    // } else {
    // // someone must have deleted the region from the initFile (with our
    // public tool?)
    // // so skip this record and don't count it as live so that the compactor
    // can gc it.
    // }
    // return true;
    // } else
    if (drs == null) { // we are not currently recovering this region
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "okToSkip because drs is null for drId={}", drId);
      }
      // Now when the diskStore is created we recover all the regions
      // immediately.
      // After that they can close and reopen a region but the close code calls
      // addUnrecoveredRegion. So I think at this point we don't need to do
      // anything.
      // // Prevent compactor from removing this oplog.
      // // It needs to be in this state until all the regions stored it in
      // // are recovered.
      // addUnrecoveredRegion(drId);
      return OkToSkipResult.SKIP_RECORD;
    }
    if (!checkRecoveryMap) {
      Object key = getRecoveryMap().get(oplogEntryId);
      if (key != null) {
        DiskEntry de = drs.getDiskEntry(key);
        if (de != null) {
          DiskId curdid = de.getDiskId();
          if (curdid != null) {
            if (curdid.getOplogId() != getOplogId()) {
              if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
                logger.trace(LogMarker.PERSIST_RECOVERY,
                    "okToSkip because getOplogId()={} != curdid.getOplogId()={} for drId={} key={}",
                    getOplogId(), curdid.getOplogId(), drId, key);
              }
              return OkToSkipResult.SKIP_RECORD;
            }
          }
        }
      }
    }
    return okToSkipRegion(drs.getDiskRegionView(), oplogEntryId, tag);
  }

  /**
   * Returns true if the drId region has been destroyed or if oplogKeyId preceeds the last clear
   * done on the drId region
   */
  private OkToSkipResult okToSkipRegion(DiskRegionView drv, long oplogKeyId, VersionTag tag) {
    long lastClearKeyId = drv.getClearOplogEntryId();
    if (lastClearKeyId != DiskStoreImpl.INVALID_ID) {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "lastClearKeyId={} oplogKeyId={}", lastClearKeyId,
            oplogKeyId);
      }
      if (lastClearKeyId >= 0) {

        if (oplogKeyId <= lastClearKeyId) {
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "okToSkip because oplogKeyId={} <= lastClearKeyId={} for drId={}", oplogKeyId,
                lastClearKeyId, drv.getId());
          }
          // @todo add some wraparound logic
          return OkToSkipResult.SKIP_RECORD;
        }
      } else {
        // lastClearKeyId is < 0 which means it wrapped around
        // treat it like an unsigned value (-1 == MAX_UNSIGNED)
        if (oplogKeyId > 0 || oplogKeyId <= lastClearKeyId) {
          // If oplogKeyId > 0 then it happened before the clear
          // (assume clear happened after we wrapped around to negative).
          // If oplogKeyId < 0 then it happened before the clear
          // if it is < lastClearKeyId
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
            logger.trace(LogMarker.PERSIST_RECOVERY,
                "okToSkip because oplogKeyId={} <= lastClearKeyId={} for drId={}", oplogKeyId,
                lastClearKeyId, drv.getId());
          }
          return OkToSkipResult.SKIP_RECORD;
        }
      }
    }
    RegionVersionVector clearRVV = drv.getClearRVV();
    if (clearRVV != null) {
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
        logger.trace(LogMarker.PERSIST_RECOVERY, "clearRVV={} tag={}", clearRVV, tag);
      }
      if (clearRVV.contains(tag.getMemberID(), tag.getRegionVersion())) {
        if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
          logger.trace(LogMarker.PERSIST_RECOVERY,
              "okToSkip because tag={} <= clearRVV={} for drId={}", tag, clearRVV, drv.getId());
        }
        // For an RVV clear, we can only skip the value during recovery
        // because later modifies may use the oplog key id.
        return OkToSkipResult.SKIP_VALUE;
      }
    }

    return OkToSkipResult.DONT_SKIP;
  }

  private long getModEntryId(CountingDataInputStream dis, int idByteCount) throws IOException {
    return calcModEntryId(getEntryIdDelta(dis, idByteCount));
  }

  private long getDelEntryId(CountingDataInputStream dis, int idByteCount) throws IOException {
    return calcDelEntryId(getEntryIdDelta(dis, idByteCount));
  }

  private/* HACK DEBUG */static long getEntryIdDelta(CountingDataInputStream dis, int idByteCount)
      throws IOException {
    assert idByteCount >= 1 && idByteCount <= 8 : idByteCount;

    long delta;
    delta = dis.readByte();
    idByteCount--;
    while (idByteCount > 0) {
      delta <<= 8;
      delta |= (0x00FF & dis.readByte());
      idByteCount--;
    }
    return delta;
  }

  /**
   * Call this when the cache is closed or region is destroyed. Deletes the lock files.
   */
  public void close() {
    if (this.closed) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Oplog::close: Store name ={} Oplog ID = {}", parent.getName(), oplogId);
    }

    basicClose(false);
  }

  /**
   * Close the files of a oplog but don't set any state. Used by unit tests
   */
  public void testClose() {
    try {
      this.crf.channel.close();
    } catch (IOException ignore) {
    }
    try {
      this.crf.raf.close();
    } catch (IOException ignore) {
    }
    this.crf.RAFClosed = true;
    try {
      this.drf.channel.close();
    } catch (IOException ignore) {
    }
    try {
      this.drf.raf.close();
    } catch (IOException ignore) {
    }
    this.drf.RAFClosed = true;
  }

  private void basicClose(boolean forceDelete) {
    flushAll();
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* crf */) {
      unpreblow(this.crf, getMaxCrfSize());
      if (!this.crf.RAFClosed) {
        try {
          this.crf.channel.close();
        } catch (IOException ignore) {
        }
        try {
          this.crf.raf.close();
        } catch (IOException ignore) {
        }
        this.crf.RAFClosed = true;
        this.stats.decOpenOplogs();
      }
      this.closed = true;
    }
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* drf */) {
      unpreblow(this.drf, getMaxDrfSize());
      if (!this.drf.RAFClosed) {
        try {
          this.drf.channel.close();
        } catch (IOException ignore) {
        }
        try {
          this.drf.raf.close();
        } catch (IOException ignore) {
        }
        this.drf.RAFClosed = true;
      }
    }

    if (forceDelete) {
      this.deleteFiles(false);
    }
  }

  /**
   * Used by tests to confirm that an oplog was compacted
   */
  boolean testConfirmCompacted() {
    return this.closed && this.deleted.get() && getOplogSize() == 0;
  }

  // @todo add state to determine when both crf and drf and been deleted.
  /**
   * Note that this can return true even when we still need to keep the oplog around because its drf
   * is still needed.
   */
  boolean isDeleted() {
    return this.deleted.get();
  }

  /**
   * Destroys this oplog. First it will call close which will cleanly close all Async threads and
   * then the oplog file will be deleted. The deletion of lock files will be taken care of by the
   * close.
   * 
   */
  public void destroy() {
    lockCompactor();
    try {
      if (!this.closed) {
        this.basicClose(true /* force delete */);
      } else {
        // do the following even if we were already closed
        deleteFiles(false);
      }
    } finally {
      unlockCompactor();
    }
  }

  /*
   * In offline compaction, after compacted each oplog, only the crf will be deleted. Oplog with drf
   * only will be housekepted later.
   */
  public void destroyCrfOnly() {
    lockCompactor();
    try {
      if (!this.closed) {
        this.basicClose(true /* force delete */);
      } else {
        // do the following even if we were already closed
        deleteFiles(true);
      }
    } finally {
      unlockCompactor();
    }
  }

  /**
   * A check to confirm that the oplog has been closed because of the cache being closed
   * 
   */
  private void checkClosed() {
    getParent().getCancelCriterion().checkCancelInProgress(null);
    if (!this.closed) {
      return;
    }
    throw new OplogCancelledException("This Oplog has been closed.");
  }

  /**
   * Return the number of bytes needed to encode the given long. Value returned will be >= 1 and <=
   * 8.
   */
  static int bytesNeeded(long v) {
    if (v < 0) {
      v = ~v;
    }
    return ((64 - Long.numberOfLeadingZeros(v)) / 8) + 1;
  }

  /**
   * Return absolute value of v.
   */
  static long abs(long v) {
    if (v < 0) {
      return -v;
    } else {
      return v;
    }
  }

  private long calcDelta(long opLogKeyID, byte opCode) {
    long delta;
    if (opCode == OPLOG_DEL_ENTRY_1ID) {
      delta = opLogKeyID - this.writeDelEntryId;
      this.writeDelEntryId = opLogKeyID;
    } else {
      delta = opLogKeyID - this.writeModEntryId;
      this.writeModEntryId = opLogKeyID;
    }
    return delta;
  }

  /**
   * This function records all the data for the current op into this.opState.
   * 
   * @param opCode The int value identifying whether it is create/modify or delete operation
   * @param entry The DiskEntry object being operated upon
   * @param value The byte array representing the value
   */
  private void initOpState(byte opCode, DiskRegionView dr, DiskEntry entry, ValueWrapper value,
      byte userBits, boolean notToUseUserBits) throws IOException {
    this.opState.initialize(opCode, dr, entry, value, userBits, notToUseUserBits);
  }

  private void clearOpState() {
    this.opState.clear();
  }

  /**
   * Returns the number of bytes it will take to serialize this.opState.
   */
  private int getOpStateSize() {
    return this.opState.getSize();
  }

  private int getOpStateValueOffset() {
    return this.opState.getValueOffset();
  }

  private byte calcUserBits(ValueWrapper vw) {
    return vw.getUserBits();
  }

  /**
   * Returns true if the given entry has not yet been written to this oplog.
   */
  private boolean modNeedsKey(DiskEntry entry) {
    DiskId did = entry.getDiskId();
    synchronized (did) {
      if (did.getOplogId() != getOplogId()) {
        // the last record for it was written in a different oplog
        // so we need the key.
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Modified the code so as to reuse the already created ByteBuffer during transition. Creates a
   * key/value pair from a region entry on disk. Updates all of the necessary
   * {@linkplain DiskStoreStats statistics} and invokes basicCreate
   * 
   * @param entry The DiskEntry object for this key/value pair.
   * @param value byte array representing the value
   */
  public void create(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {

    if (this != getOplogSet().getChild()) {
      getOplogSet().getChild().create(region, entry, value, async);
    } else {
      DiskId did = entry.getDiskId();
      boolean exceptionOccurred = false;
      byte prevUsrBit = did.getUserBits();
      int len = did.getValueLength();
      try {
        // It is ok to do this outside of "lock" because
        // create records do not need to change.
        byte userBits = calcUserBits(value);
        // save versions for creates and updates even if value is bytearrary in
        // 7.0
        if (entry.getVersionStamp() != null) {
          if (entry.getVersionStamp().getMemberID() == null) {
            throw new AssertionError(
                "Version stamp should have a member at this point for entry " + entry);
          }
          // pdx and tx will not use version
          userBits = EntryBits.setWithVersions(userBits, true);
        }
        basicCreate(region.getDiskRegion(), entry, value, userBits, async);
      } catch (IOException ex) {
        exceptionOccurred = true;
        region.getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0
            .toLocalizedString(this.diskFile.getPath()), ex, region.getFullPath());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        exceptionOccurred = true;
        region.getCancelCriterion().checkCancelInProgress(ie);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
                .toLocalizedString(this.diskFile.getPath()),
            ie, region.getFullPath());
      } finally {
        if (exceptionOccurred) {
          did.setValueLength(len);
          did.setUserBits(prevUsrBit);
        }

      }

    }

  }

  /**
   * Return true if no records have been written to the oplog yet.
   */
  private boolean isFirstRecord() {
    return this.firstRecord;
  }

  /**
   * A helper function which identifies whether to create the entry in the current oplog or to make
   * the switch to the next oplog. This function enables us to reuse the byte buffer which got
   * created for an oplog which no longer permits us to use itself
   * 
   * @param entry DiskEntry object representing the current Entry
   */
  private void basicCreate(DiskRegion dr, DiskEntry entry, ValueWrapper value, byte userBits,
      boolean async) throws IOException, InterruptedException {
    DiskId id = entry.getDiskId();
    boolean useNextOplog = false;
    long startPosForSynchOp = -1;
    if (DiskStoreImpl.KRF_DEBUG) {
      // wait for cache close to create krf
      System.out.println("basicCreate KRF_DEBUG");
      Thread.sleep(1000);
    }
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) { // TODO soplog perf analysis shows this as a
        // contention point
        // synchronized (this.crf) {
        initOpState(OPLOG_NEW_ENTRY_0ID, dr, entry, value, userBits, false);
        // Check if the current data in ByteBuffer will cause a
        // potential increase in the size greater than the max allowed
        long temp = (getOpStateSize() + this.crf.currSize);
        if (!this.wroteNewEntryBase) {
          temp += OPLOG_NEW_ENTRY_BASE_REC_SIZE;
        }
        if (this != getOplogSet().getChild()) {
          useNextOplog = true;
        } else if (temp > getMaxCrfSize() && !isFirstRecord()) {
          switchOpLog(dr, getOpStateSize(), entry);
          useNextOplog = true;
        } else {
          if (this.lockedForKRFcreate) {
            CacheClosedException cce = new CacheClosedException("The disk store is closed.");
            dr.getCancelCriterion().checkCancelInProgress(cce);
            throw cce;
          }
          this.firstRecord = false;
          writeNewEntryBaseRecord(async);
          // Now we can finally call newOplogEntryId.
          // We need to make sure the create records
          // are written in the same order as they are created.
          // This allows us to not encode the oplogEntryId explicitly in the
          // record
          long createOplogEntryId = getOplogSet().newOplogEntryId();
          id.setKeyId(createOplogEntryId);

          // startPosForSynchOp = this.crf.currSize;
          // Allow it to be added to the OpLOg so increase the
          // size of currenstartPosForSynchOpt oplog
          int dataLength = getOpStateSize();
          // It is necessary that we set the
          // Oplog ID here without releasing the lock on object as we are
          // writing to the file after releasing the lock. This can cause
          // a situation where the
          // switching thread has added Oplog for compaction while the previous
          // thread has still not started writing. Thus compactor can
          // miss an entry as the oplog Id was not set till then.
          // This is because a compactor thread will iterate over the entries &
          // use only those which have OplogID equal to that of Oplog being
          // compacted without taking any lock. A lock is taken only if the
          // entry is a potential candidate.
          // Further the compactor may delete the file as a compactor thread does
          // not require to take any shared/exclusive lock at DiskStoreImpl
          // or Oplog level.
          // It is also assumed that compactor thread will take a lock on both
          // entry as well as DiskID while compacting. In case of synch
          // mode we can
          // safely set OplogID without taking lock on DiskId. But
          // for asynch mode
          // we have to take additional precaution as the asynch
          // writer of previous
          // oplog can interfere with the current oplog.
          id.setOplogId(getOplogId());
          // do the io while holding lock so that switch can set doneAppending
          // Write the data to the opLog for the synch mode
          startPosForSynchOp = writeOpLogBytes(this.crf, async, true);
          // if (this.crf.currSize != startPosForSynchOp) {
          // assert false;
          // }
          this.crf.currSize = temp;
          if (EntryBits.isNeedsValue(userBits)) {
            id.setValueLength(value.getLength());
          } else {
            id.setValueLength(0);
          }
          id.setUserBits(userBits);

          if (logger.isTraceEnabled()) {
            logger.trace("Oplog::basicCreate:Release dByteBuffer with data for Disk ID = {}", id);
          }
          // As such for any put or get operation , a synch is taken
          // on the Entry object in the DiskEntry's Helper functions.
          // Compactor thread will also take a lock on entry object. Therefore
          // we do not require a lock on DiskID, as concurrent access for
          // value will not occur.
          startPosForSynchOp += getOpStateValueOffset();
          if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
            VersionTag tag = null;
            if (entry.getVersionStamp() != null) {
              tag = entry.getVersionStamp().asVersionTag();
            }
            logger.trace(LogMarker.PERSIST_WRITES,
                "basicCreate: id=<{}> key=<{}> valueOffset={} userBits={} valueLen={} valueBytes={} drId={} versionTag={} oplog#{}",
                abs(id.getKeyId()), entry.getKey(), startPosForSynchOp, userBits,
                (value != null ? value.getLength() : 0), value.getBytesAsString(), dr.getId(), tag,
                getOplogId());
          }
          id.setOffsetInOplog(startPosForSynchOp);
          addLive(dr, entry);
          // Size of the current oplog being increased
          // due to 'create' operation. Set the change in stats.
          this.dirHolder.incrementTotalOplogSize(dataLength);
          incTotalCount();

          // Update the region version vector for the disk store.
          // This needs to be done under lock so that we don't switch oplogs
          // unit the version vector accurately represents what is in this oplog
          RegionVersionVector rvv = dr.getRegionVersionVector();
          if (rvv != null && entry.getVersionStamp() != null) {
            rvv.recordVersion(entry.getVersionStamp().getMemberID(),
                entry.getVersionStamp().getRegionVersion());
          }

          EntryLogger.logPersistPut(dr.getName(), entry.getKey(), dr.getDiskStoreID());
        }
        clearOpState();
        // }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(this != getOplogSet().getChild());
      getOplogSet().getChild().basicCreate(dr, entry, value, userBits, async);
    } else {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSettingOplogOffSet(startPosForSynchOp);
      }
    }
  }

  private static byte[] TOMBSTONE_BYTES;
  static {
    try {
      TOMBSTONE_BYTES = BlobHelper.serializeToBlob(Token.TOMBSTONE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This oplog will be forced to switch to a new oplog
   */
  void forceRolling(DiskRegion dr) {
    if (getOplogSet().getChild() == this) {
      getParent().getBackupLock().lock();
      try {
        synchronized (this.lock) {
          if (getOplogSet().getChild() == this) {
            switchOpLog(dr, 0, null);
          }
        }
      } finally {
        getParent().getBackupLock().unlock();
      }
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
    }
  }

  /**
   * Return true if it is possible that compaction of this oplog will be done.
   */
  private boolean isCompactionPossible() {
    return getOplogSet().isCompactionPossible();
  }

  /**
   * This function is used to switch from one op Log to another , when the size of the current oplog
   * has reached the maximum permissible. It is always called from synch block with lock object
   * being the OpLog File object We will reuse the ByteBuffer Pool. We should add the current Oplog
   * for compaction first & then try to get next directory holder as in case there is only a single
   * directory with space being full, compaction has to happen before it can be given a new
   * directory. If the operation causing the switching is on an Entry which already is referencing
   * the oplog to be compacted, then the compactor thread will skip compaction that entry & the
   * switching thread will roll the entry explicitly.
   * 
   * @param lengthOfOperationCausingSwitch length of the operation causing the switch
   * @param entryCausingSwitch DiskEntry object operation on which caused the switching of Oplog.
   *        This can be null if the switching has been invoked by the forceRolling which does not
   *        need an operation on entry to cause the switch
   */
  private void switchOpLog(DiskRegionView dr, int lengthOfOperationCausingSwitch,
      DiskEntry entryCausingSwitch) {
    String drName;
    if (dr != null) {
      drName = dr.getName();
    } else {
      drName = getParent().getName();
    }
    flushAll(); // needed in case of async
    lengthOfOperationCausingSwitch += 20; // for worstcase overhead of writing
                                          // first record

    // if length of operation is greater than max Dir Size than an exception is
    // thrown

    if (logger.isDebugEnabled()) {
      logger.debug("Oplog::switchOpLog: Entry causing Oplog switch has diskID={}",
          (entryCausingSwitch != null ? entryCausingSwitch.getDiskId() : "Entry is null"));
    }
    if (lengthOfOperationCausingSwitch > getParent().getMaxDirSize()) {
      throw new DiskAccessException(
          LocalizedStrings.Oplog_OPERATION_SIZE_CANNOT_EXCEED_THE_MAXIMUM_DIRECTORY_SIZE_SWITCHING_PROBLEM_FOR_ENTRY_HAVING_DISKID_0
              .toLocalizedString((entryCausingSwitch != null
                  ? entryCausingSwitch.getDiskId().toString() : "\"null Entry\"")),
          drName);
    }
    if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      CacheObserverHolder.getInstance().beforeSwitchingOplog();
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Oplog::switchOpLog: About to add the Oplog = {} for compaction. Entry causing the switch is having DiskID = {}",
          this.oplogId,
          (entryCausingSwitch != null ? entryCausingSwitch.getDiskId() : "null Entry"));
    }
    if (needsCompaction()) {
      addToBeCompacted();
    } else {
      getOplogSet().addInactive(this);
    }

    try {
      DirectoryHolder nextDirHolder =
          getOplogSet().getNextDir(lengthOfOperationCausingSwitch, true);
      Oplog newOplog = new Oplog(this.oplogId + 1, nextDirHolder, this);
      newOplog.firstRecord = true;
      getOplogSet().setChild(newOplog);

      finishedAppending();

      // Defer the unpreblow and close of the RAF. We saw pauses in testing from
      // unpreblow of the drf, maybe because it is freeing pages that were
      // preallocated. Close can pause if another thread is calling force on the
      // file channel - see 50254. These operations should be safe to defer,
      // a concurrent read will synchronize on the oplog and use or reopen the
      // RAF
      // as needed.
      getParent().executeDelayedExpensiveWrite(new Runnable() {
        public void run() {
          // need to truncate crf and drf if their actual size is less than
          // their pregrow size
          unpreblow(Oplog.this.crf, getMaxCrfSize());
          unpreblow(Oplog.this.drf, getMaxDrfSize());
          // Close the crf using closeRAF. We will reopen the crf if we
          // need it to fault in or to read values during compaction.
          closeRAF();
          // I think at this point the drf no longer needs to be open
          synchronized (Oplog.this.lock/* drf */) {
            if (!Oplog.this.drf.RAFClosed) {
              try {
                Oplog.this.drf.channel.close();
              } catch (IOException ignore) {
              }
              try {
                Oplog.this.drf.raf.close();
              } catch (IOException ignore) {
              }
              Oplog.this.drf.RAFClosed = true;
            }
          }

        }
      });

      // offline compaction will not execute create Krf in the task, becasue
      // this.totalLiveCount.get() == 0
      if (getParent().isOfflineCompacting()) {
        krfClose();
      } else {
        createKrfAsync();
      }
    } catch (DiskAccessException dae) {
      // Remove the Oplog which was added in the DiskStoreImpl
      // for compaction as compaction cannot be done.
      // However, it is also possible that compactor
      // may have done the compaction of the Oplog but the switching thread
      // encountered timeout exception.
      // So the code below may or may not actually
      // ensure that the Oplog has been compacted or not.
      getOplogSet().removeOplog(this.oplogId);
      throw dae;
    }
  }

  /**
   * Schedule a task to create a krf asynchronously
   */
  protected void createKrfAsync() {
    getParent().executeDiskStoreTask(new Runnable() {
      public void run() {
        createKrf(false);
      }
    });
  }

  /**
   * Used when creating a KRF to keep track of what DiskRegionView a DiskEntry belongs to.
   */
  private static class KRFEntry {
    private final DiskEntry de;
    private final DiskRegionView drv;
    /**
     * Fix for 42733 - a stable snapshot of the offset so we can sort It doesn't matter that this is
     * stale, we'll filter out these entries later.
     */
    private final long offsetInOplog;
    private VersionHolder versionTag;

    public KRFEntry(DiskRegionView drv, DiskEntry de, VersionHolder tag) {
      this.de = de;
      this.drv = drv;
      DiskId diskId = de.getDiskId();
      this.offsetInOplog = diskId != null ? diskId.getOffsetInOplog() : 0;
      this.versionTag = tag;
    }

    public DiskEntry getDiskEntry() {
      return this.de;
    }

    public DiskRegionView getDiskRegionView() {
      return this.drv;
    }

    public long getOffsetInOplogForSorting() {
      return offsetInOplog;
    }
  }

  private void writeOneKeyEntryForKRF(KRFEntry ke) throws IOException {
    DiskEntry de = ke.getDiskEntry();
    long diskRegionId = ke.getDiskRegionView().getId();
    long oplogKeyId;
    byte userBits;
    long valueOffset;
    int valueLength;
    Object deKey;
    VersionHolder tag = ke.versionTag;

    synchronized (de) {
      DiskId di = de.getDiskId();
      if (di == null) {
        return;
      }
      if (de.isRemovedFromDisk()) {
        // the entry was concurrently removed
        return;
      }
      synchronized (di) {
        // Make sure each one is still in this oplog.
        if (di.getOplogId() != getOplogId()) {
          return;
        }
        userBits = di.getUserBits();
        oplogKeyId = di.getKeyId();
        valueOffset = di.getOffsetInOplog();
        valueLength = di.getValueLength();
        deKey = de.getKey();
        if (valueOffset < 0) {
          assert (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits));
        }
      }

      if (tag == null) {
        if (EntryBits.isWithVersions(userBits) && de.getVersionStamp() != null) {
          tag = de.getVersionStamp().asVersionTag();
        } else if (de.getVersionStamp() != null) {
          throw new AssertionError("No version bits on entry we're writing to the krf " + de);
        }
      }
    }
    if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
      logger.trace(LogMarker.PERSIST_WRITES,
          "krf oplogId={} key={} oplogKeyId={} de={} vo={} vl={} diskRegionId={} version tag={}",
          oplogId, deKey, oplogKeyId, System.identityHashCode(de), valueOffset, valueLength,
          diskRegionId, tag);
    }
    byte[] keyBytes = EntryEventImpl.serialize(deKey);

    // skip the invalid entries, theire valueOffset is -1
    writeOneKeyEntryForKRF(keyBytes, userBits, valueLength, diskRegionId, oplogKeyId, valueOffset,
        tag);
  }

  private void writeOneKeyEntryForKRF(byte[] keyBytes, byte userBits, int valueLength,
      long diskRegionId, long oplogKeyId, long valueOffset, VersionHolder tag) throws IOException {
    if (getParent().isValidating()) {
      return;
    }

    if (!getParent().isOfflineCompacting()) {
      assert (this.krf.dos != null);
    } else {
      if (this.krf.dos == null) {
        // krf already exist, thus not re-opened for write
        return;
      }
    }
    DataSerializer.writeByteArray(keyBytes, this.krf.dos);
    this.krf.dos.writeByte(EntryBits.getPersistentBits(userBits));
    InternalDataSerializer.writeArrayLength(valueLength, this.krf.dos);
    DiskInitFile.writeDiskRegionID(this.krf.dos, diskRegionId);
    if (EntryBits.isWithVersions(userBits) && tag != null) {
      serializeVersionTag(tag, this.krf.dos);
    }
    InternalDataSerializer.writeVLOld(oplogKeyId, this.krf.dos);
    // skip the invalid entries, theire valueOffset is -1
    if (!EntryBits.isAnyInvalid(userBits) && !EntryBits.isTombstone(userBits)) {
      InternalDataSerializer.writeVLOld((valueOffset - this.krf.lastOffset), this.krf.dos);
      // save the lastOffset in krf object
      this.krf.lastOffset = valueOffset;
    }
    this.krf.keyNum++;
  }

  private final AtomicBoolean krfCreated = new AtomicBoolean();

  public void krfFileCreate() throws IOException {
    // this method is only used by offline compaction. validating will not
    // create krf
    assert (getParent().isValidating() == false);

    this.krf.f = new File(this.diskFile.getPath() + KRF_FILE_EXT);
    if (this.krf.f.exists()) {
      throw new IllegalStateException("krf file " + this.krf.f + " already exists.");
    }
    this.krf.fos = new FileOutputStream(this.krf.f);
    this.krf.bos = new BufferedOutputStream(this.krf.fos, 32768);
    this.krf.dos = new DataOutputStream(this.krf.bos);

    // write oplog magic seq
    this.krf.dos.writeByte(OPLOG_MAGIC_SEQ_ID);
    this.krf.dos.write(Oplog.OPLOG_TYPE.KRF.getBytes(), 0, Oplog.OPLOG_TYPE.getLen());
    this.krf.dos.writeByte(END_OF_RECORD_ID);

    // write the disk store id to the krf
    this.krf.dos.writeByte(OPLOG_DISK_STORE_ID);
    this.krf.dos.writeLong(getParent().getDiskStoreID().getLeastSignificantBits());
    this.krf.dos.writeLong(getParent().getDiskStoreID().getMostSignificantBits());
    this.krf.dos.writeByte(END_OF_RECORD_ID);

    // write product versions
    assert this.gfversion != null;
    // write both gemfire and data versions if the two are different else write
    // only gemfire version; a token distinguishes the two cases while reading
    // like in writeGemFireVersionRecord
    Version dataVersion = getDataVersionIfOld();
    if (dataVersion == null) {
      dataVersion = Version.CURRENT;
    }
    if (this.gfversion == dataVersion) {
      this.gfversion.writeOrdinal(this.krf.dos, false);
    } else {
      Version.TOKEN.writeOrdinal(this.krf.dos, false);
      this.krf.dos.writeByte(END_OF_RECORD_ID);
      this.krf.dos.writeByte(OPLOG_GEMFIRE_VERSION);
      this.gfversion.writeOrdinal(this.krf.dos, false);
      this.krf.dos.writeByte(END_OF_RECORD_ID);
      this.krf.dos.writeByte(OPLOG_GEMFIRE_VERSION);
      dataVersion.writeOrdinal(this.krf.dos, false);
    }
    this.krf.dos.writeByte(END_OF_RECORD_ID);

    // Write the total entry count to the krf so that when we recover,
    // our compaction statistics will be accurate
    InternalDataSerializer.writeUnsignedVL(this.totalCount.get(), this.krf.dos);
    this.krf.dos.writeByte(END_OF_RECORD_ID);

    // Write the RVV to the krf.
    Map<Long, AbstractDiskRegion> drMap = getParent().getAllDiskRegions();
    byte[] rvvBytes = serializeRVVs(drMap, false);
    this.krf.dos.write(rvvBytes);
    this.krf.dos.writeByte(END_OF_RECORD_ID);
  }

  // if IOException happened during krf creation, close and delete it
  private void closeAndDeleteKrf() {
    try {
      if (this.krf.dos != null) {
        this.krf.dos.close();
        this.krf.dos = null;
      }
    } catch (IOException ignore) {
    }
    try {
      if (this.krf.bos != null) {
        this.krf.bos.close();
        this.krf.bos = null;
      }
    } catch (IOException ignore) {
    }
    try {
      if (this.krf.fos != null) {
        this.krf.fos.close();
        this.krf.fos = null;
      }
    } catch (IOException ignore) {
    }

    if (this.krf.f.exists()) {
      this.krf.f.delete();
    }
  }

  public void krfClose() {
    boolean allClosed = false;
    try {
      if (this.krf.fos != null) {
        DataSerializer.writeByteArray(null, this.krf.dos);
      } else {
        return;
      }

      this.krf.dos.flush();
      this.krf.fos.getChannel().force(true);

      this.krf.dos.close();
      this.krf.dos = null;
      this.krf.bos.close();
      this.krf.bos = null;
      this.krf.fos.close();
      this.krf.fos = null;

      if (this.krf.keyNum == 0) {
        // this is an empty krf file
        this.krf.f.delete();
        assert this.krf.f.exists() == false;
      } else {
        // Mark that this krf is complete.
        getParent().getDiskInitFile().krfCreate(this.oplogId);
        logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_CREATE_0_1_2,
            new Object[] {toString(), "krf", getParent().getName()}));
      }

      allClosed = true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      if (getParent().getDiskAccessException() == null) {
        throw new DiskAccessException("Fail to close krf file " + this.krf.f, e, getParent());
      } else {
        logger.info(
            "Fail to close krf file " + this.krf.f + ", but a DiskAccessException happened ealier",
            getParent().getDiskAccessException());
      }
    } finally {
      if (!allClosed) {
        // IOException happened during close, delete this krf
        closeAndDeleteKrf();
      }
    }
  }

  /**
   * Create the KRF file for this oplog. It is ok for this method to be async. finishKRF will be
   * called and it must block until KRF generation is complete.
   * 
   * @param cancel if true then prevent the krf from being created if possible
   */
  void createKrf(boolean cancel) {
    if (cancel) {
      this.krfCreated.compareAndSet(false, true);
      return;
    }
    if (!couldHaveKrf()) {
      return;
    }
    // Make sure regions can not become unrecovered while creating the KRF.
    getParent().acquireCompactorReadLock();
    try {
      if (!getParent().allowKrfCreation()) {
        return;
      }
      lockCompactor();

      try {
        // No need to get the backup lock prior to synchronizing (correct lock order) since the
        // synchronized block does not attempt to get the backup lock (incorrect lock order)
        synchronized (this.lock) {
          // 42733: after set it to true, we will not reset it, since this oplog
          // will be
          // inactive forever
          this.lockedForKRFcreate = true;
        }
        synchronized (this.krfCreated) {
          if (this.krfCreated.get()) {
            return;
          }
          this.krfCreated.set(true);

          if (this.unrecoveredRegionCount.get() > 0) {
            // if we have unrecovered regions then we can't create
            // a KRF because we don't have the list of live entries.
            return;
          }

          int tlc = (int) this.totalLiveCount.get();
          if (tlc <= 0) {
            // no need to create a KRF since this oplog will be deleted.
            // TODO should we create an empty KRF anyway?
            return;
          }

          Collection<DiskRegionInfo> regions = this.regionMap.values();
          List<KRFEntry> sortedLiveEntries = getSortedLiveEntries(regions);
          if (sortedLiveEntries == null) {
            // no need to create a krf if there are no live entries.
            return;
          }

          boolean krfCreateSuccess = false;
          try {
            krfFileCreate();

            // sortedLiveEntries are now sorted
            // so we can start writing them to disk.
            for (KRFEntry ke : sortedLiveEntries) {
              writeOneKeyEntryForKRF(ke);
            }

            krfClose();
            krfCreateSuccess = true;
            for (DiskRegionInfo dri : regions) {
              dri.afterKrfCreated();
            }
            if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
              CacheObserverHolder.getInstance().afterKrfCreated();
            }
          } catch (FileNotFoundException ex) {
            // TODO handle exception; we couldn't open the krf file
            throw new IllegalStateException("could not create krf " + this.krf.f, ex);
          } catch (IOException ex) {
            // TODO handle io exceptions; we failed to write to the file
            throw new IllegalStateException("failed writing krf " + this.krf.f, ex);
          } finally {
            // if IOException happened in writeOneKeyEntryForKRF(), delete krf
            // here
            if (!krfCreateSuccess) {
              closeAndDeleteKrf();
            }
          }
        }
      } finally {
        unlockCompactor();
      }
    } finally {
      getParent().releaseCompactorReadLock();
    }
  }

  File getKrfFile() {
    return new File(this.diskFile.getPath() + KRF_FILE_EXT);
  }

  public List<KRFEntry> getSortedLiveEntries(Collection<DiskRegionInfo> targetRegions) {
    int tlc = (int) this.totalLiveCount.get();
    if (tlc <= 0) {
      // no need to create a KRF since this oplog will be deleted.
      // TODO should we create an empty KRF anyway?
      return null;
    }

    KRFEntry[] sortedLiveEntries = new KRFEntry[tlc];
    int idx = 0;
    for (DiskRegionInfo dri : targetRegions) {
      if (dri.getDiskRegion() != null) {
        idx = dri.addLiveEntriesToList(sortedLiveEntries, idx);
      }
    }
    // idx is now the length of sortedLiveEntries
    Arrays.sort(sortedLiveEntries, 0, idx, new Comparator<KRFEntry>() {
      public int compare(KRFEntry o1, KRFEntry o2) {
        long val1 = o1.getOffsetInOplogForSorting();
        long val2 = o2.getOffsetInOplogForSorting();
        return Long.signum(val1 - val2);
      }
    });

    return Arrays.asList(sortedLiveEntries).subList(0, idx);
  }

  /**
   * This function retrieves the value for an entry being compacted subject to entry referencing the
   * oplog being compacted. Attempt is made to retrieve the value from in memory , if available,
   * else from asynch buffers ( if asynch mode is enabled), else from the Oplog being compacted. It
   * is invoked from switchOplog as well as OplogCompactor's compact function.
   * 
   * @param entry DiskEntry being compacted referencing the Oplog being compacted
   * @param wrapper Object of type BytesAndBitsForCompactor. The data if found is set in the wrapper
   *        Object. The wrapper Object also contains the user bit associated with the entry
   * @return boolean false indicating that entry need not be compacted. If true it means that
   *         wrapper has been appropriately filled with data
   */
  private boolean getBytesAndBitsForCompaction(DiskRegionView dr, DiskEntry entry,
      BytesAndBitsForCompactor wrapper) {
    // caller is synced on did
    DiskId did = entry.getDiskId();
    byte userBits = 0;
    long oplogOffset = did.getOffsetInOplog();
    ReferenceCountHelper.skipRefCountTracking();
    @Retained
    @Released
    Object value = entry._getValueRetain(dr, true);
    ReferenceCountHelper.unskipRefCountTracking();
    boolean foundData = false;
    if (value == null) {
      // If the mode is synch it is guaranteed to be present in the disk
      foundData = basicGetForCompactor(dr, oplogOffset, false, did.getValueLength(),
          did.getUserBits(), wrapper);
      // after we have done the get do one more check to see if the
      // disk id of interest is still stored in the current oplog.
      // Do this to fix bug 40648
      // Since we now call this with the diskId synced I think
      // it is impossible for this oplogId to change.
      if (did.getOplogId() != getOplogId()) {
        // if it is not then no need to compact it
        return false;
      } else {
        // if the disk id indicates its most recent value is in oplogInFocus
        // then we should have found data
        assert foundData : "compactor get failed on oplog#" + getOplogId();
      }
      userBits = wrapper.getBits();
      if (EntryBits.isAnyInvalid(userBits)) {
        if (EntryBits.isInvalid(userBits)) {
          wrapper.setData(DiskEntry.INVALID_BYTES, userBits, DiskEntry.INVALID_BYTES.length,
              false/*
                    * Can not be reused
                    */);
        } else {
          wrapper.setData(DiskEntry.LOCAL_INVALID_BYTES, userBits,
              DiskEntry.LOCAL_INVALID_BYTES.length, false/*
                                                          * Can not be reused
                                                          */);
        }
      } else if (EntryBits.isTombstone(userBits)) {
        wrapper.setData(DiskEntry.TOMBSTONE_BYTES, userBits, DiskEntry.TOMBSTONE_BYTES.length,
            false/*
                  * Can not be reused
                  */);
      }
      if (EntryBits.isWithVersions(did.getUserBits())) {
        userBits = EntryBits.setWithVersions(userBits, true);
      }
    } else {
      foundData = true;
      userBits = 0;
      if (EntryBits.isRecoveredFromDisk(did.getUserBits())) {
        userBits = EntryBits.setRecoveredFromDisk(userBits, true);
      }
      if (EntryBits.isWithVersions(did.getUserBits())) {
        userBits = EntryBits.setWithVersions(userBits, true);
      }
      // no need to preserve pendingAsync bit since we will clear it anyway
      // since we
      // (the compactor) are writing the value out to disk.
      if (value == Token.INVALID) {
        userBits = EntryBits.setInvalid(userBits, true);
        wrapper.setData(DiskEntry.INVALID_BYTES, userBits, DiskEntry.INVALID_BYTES.length,
            false /*
                   * Cannot be reused
                   */);

      } else if (value == Token.LOCAL_INVALID) {
        userBits = EntryBits.setLocalInvalid(userBits, true);
        wrapper.setData(DiskEntry.LOCAL_INVALID_BYTES, userBits,
            DiskEntry.LOCAL_INVALID_BYTES.length, false /*
                                                         * Cannot be reused
                                                         */);
      } else if (value == Token.TOMBSTONE) {
        userBits = EntryBits.setTombstone(userBits, true);
        wrapper.setData(DiskEntry.TOMBSTONE_BYTES, userBits, DiskEntry.TOMBSTONE_BYTES.length,
            false /*
                   * Cannot be reused
                   */);
      } else if (value instanceof CachedDeserializable) {
        CachedDeserializable proxy = (CachedDeserializable) value;
        if (proxy instanceof StoredObject) {
          @Released
          StoredObject ohproxy = (StoredObject) proxy;
          try {
            ohproxy.fillSerializedValue(wrapper, userBits);
          } finally {
            OffHeapHelper.releaseWithNoTracking(ohproxy);
          }
        } else {
          userBits = EntryBits.setSerialized(userBits, true);
          proxy.fillSerializedValue(wrapper, userBits);
        }
      } else if (value instanceof byte[]) {
        byte[] valueBytes = (byte[]) value;
        // If the value is already a byte array then the user bit
        // is 0, which is the default value of the userBits variable,
        // indicating that it is non serialized data. Thus it is
        // to be used as it is & not to be deserialized to
        // convert into Object
        wrapper.setData(valueBytes, userBits, valueBytes.length,
            false /*
                   * the wrapper is not reusable
                   */);
      } else if (Token.isRemoved(value) && value != Token.TOMBSTONE) {
        // TODO - RVV - We need to handle tombstones differently here!
        if (entry.getDiskId().isPendingAsync()) {
          entry.getDiskId().setPendingAsync(false);
          try {
            getOplogSet().getChild().basicRemove(dr, entry, false, false);
          } catch (IOException ex) {
            getParent().getCancelCriterion().checkCancelInProgress(ex);
            throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0
                .toLocalizedString(this.diskFile.getPath()), ex, dr.getName());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            getParent().getCache().getCancelCriterion().checkCancelInProgress(ie);
            throw new DiskAccessException(
                LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
                    .toLocalizedString(this.diskFile.getPath()),
                ie, dr.getName());
          }
        } else {
          rmLive(dr, entry);
        }
        foundData = false;
      } else {
        userBits = EntryBits.setSerialized(userBits, true);
        EntryEventImpl.fillSerializedValue(wrapper, value, userBits);
      }
    }
    if (foundData) {
      // since the compactor is writing it out clear the async flag
      entry.getDiskId().setPendingAsync(false);
    }
    return foundData;
  }

  /**
   * Modifies a key/value pair from a region entry on disk. Updates all of the necessary
   * {@linkplain DiskStoreStats statistics} and invokes basicModify
   * <p>
   * Modified the code so as to reuse the already created ByteBuffer during transition. Minimizing
   * the synchronization allowing multiple put operations for different entries to proceed
   * concurrently for asynch mode
   * 
   * @param entry DiskEntry object representing the current Entry
   * 
   * @param value byte array representing the value
   */
  public void modify(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    if (getOplogSet().getChild() != this) {
      getOplogSet().getChild().modify(region, entry, value, async);
    } else {
      DiskId did = entry.getDiskId();
      boolean exceptionOccurred = false;
      byte prevUsrBit = did.getUserBits();
      int len = did.getValueLength();
      try {
        byte userBits = calcUserBits(value);
        // save versions for creates and updates even if value is bytearrary in
        // 7.0
        if (entry.getVersionStamp() != null) {
          if (entry.getVersionStamp().getMemberID() == null) {
            throw new AssertionError(
                "Version stamp should have a member at this point for entry " + entry);
          }
          // pdx and tx will not use version
          userBits = EntryBits.setWithVersions(userBits, true);
        }
        basicModify(region.getDiskRegion(), entry, value, userBits, async, false);
      } catch (IOException ex) {
        exceptionOccurred = true;
        region.getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0
            .toLocalizedString(this.diskFile.getPath()), ex, region.getFullPath());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        exceptionOccurred = true;
        region.getCancelCriterion().checkCancelInProgress(ie);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
                .toLocalizedString(this.diskFile.getPath()),
            ie, region.getFullPath());
      } finally {
        if (exceptionOccurred) {
          did.setValueLength(len);
          did.setUserBits(prevUsrBit);
        }
      }
    }
  }

  public void offlineModify(DiskRegionView drv, DiskEntry entry, byte[] value,
      boolean isSerializedObject) {
    try {
      ValueWrapper vw = new DiskEntry.Helper.ByteArrayValueWrapper(isSerializedObject, value);
      byte userBits = calcUserBits(vw);
      // save versions for creates and updates even if value is bytearrary in 7.0
      VersionStamp vs = entry.getVersionStamp();
      if (vs != null) {
        if (vs.getMemberID() == null) {
          throw new AssertionError(
              "Version stamp should have a member at this point for entry " + entry);
        }
        // Since we are modifying this entry's value while offline make sure its version stamp
        // has this disk store as its member id and bump the version
        vs.setMemberID(getParent().getDiskStoreID());
        VersionTag vt = vs.asVersionTag();
        vt.setRegionVersion(drv.getRegionVersionVector().getNextVersion());
        vt.setEntryVersion(vt.getEntryVersion() + 1);
        vt.setVersionTimeStamp(System.currentTimeMillis());
        vs.setVersions(vt);
        userBits = EntryBits.setWithVersions(userBits, true);
      }
      basicModify(drv, entry, vw, userBits, false, false);
    } catch (IOException ex) {
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0.toLocalizedString(this.diskFile.getPath()),
          ex, drv.getName());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
              .toLocalizedString(this.diskFile.getPath()),
          ie, drv.getName());
    }
  }

  public void saveConflictVersionTag(LocalRegion region, VersionTag tag, boolean async) {
    if (getOplogSet().getChild() != this) {
      getOplogSet().getChild().saveConflictVersionTag(region, tag, async);
    } else {
      try {
        basicSaveConflictVersionTag(region.getDiskRegion(), tag, async);
      } catch (IOException ex) {
        region.getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_CONFLICT_VERSION_TAG_0
            .toLocalizedString(this.diskFile.getPath()), ex, region.getFullPath());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        region.getCancelCriterion().checkCancelInProgress(ie);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_CONFLICT_VERSION_TAG_0
            .toLocalizedString(this.diskFile.getPath()), ie, region.getFullPath());
      }
    }
  }

  private void copyForwardForOfflineCompact(long oplogKeyId, byte[] keyBytes, byte[] valueBytes,
      byte userBits, long drId, VersionTag tag) {
    try {
      basicCopyForwardForOfflineCompact(oplogKeyId, keyBytes, valueBytes, userBits, drId, tag);
    } catch (IOException ex) {
      getParent().getCancelCriterion().checkCancelInProgress(ex);
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0.toLocalizedString(this.diskFile.getPath()),
          ex, getParent());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      getParent().getCancelCriterion().checkCancelInProgress(ie);
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
              .toLocalizedString(this.diskFile.getPath()),
          ie, getParent());
    }
  }

  private void copyForwardModifyForCompact(DiskRegionView dr, DiskEntry entry,
      BytesAndBitsForCompactor wrapper) {
    if (getOplogSet().getChild() != this) {
      getOplogSet().getChild().copyForwardModifyForCompact(dr, entry, wrapper);
    } else {
      DiskId did = entry.getDiskId();
      boolean exceptionOccurred = false;
      int len = did.getValueLength();
      try {
        // TODO: compaction needs to get version?
        byte userBits = wrapper.getBits();
        ValueWrapper vw;
        if (wrapper.getOffHeapData() != null) {
          vw = new DiskEntry.Helper.OffHeapValueWrapper(wrapper.getOffHeapData());
        } else {
          vw = new DiskEntry.Helper.CompactorValueWrapper(wrapper.getBytes(),
              wrapper.getValidLength());
        }
        // Compactor always says to do an async basicModify so that its writes
        // will be grouped. This is not a true async write; just a grouped one.
        basicModify(dr, entry, vw, userBits, true, true);
      } catch (IOException ex) {
        exceptionOccurred = true;
        getParent().getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0
            .toLocalizedString(this.diskFile.getPath()), ex, getParent());
      } catch (InterruptedException ie) {
        exceptionOccurred = true;
        Thread.currentThread().interrupt();
        getParent().getCancelCriterion().checkCancelInProgress(ie);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
                .toLocalizedString(this.diskFile.getPath()),
            ie, getParent());
      } finally {
        if (wrapper.getOffHeapData() != null) {
          wrapper.setOffHeapData(null, (byte) 0);
        }
        if (exceptionOccurred) {
          did.setValueLength(len);
        }
      }
    }
  }

  /**
   * A helper function which identifies whether to modify the entry in the current oplog or to make
   * the switch to the next oplog. This function enables us to reuse the byte buffer which got
   * created for an oplog which no longer permits us to use itself. It will also take acre of
   * compaction if required
   * 
   * @param entry DiskEntry object representing the current Entry
   */
  private void basicModify(DiskRegionView dr, DiskEntry entry, ValueWrapper value, byte userBits,
      boolean async, boolean calledByCompactor) throws IOException, InterruptedException {
    DiskId id = entry.getDiskId();
    boolean useNextOplog = false;
    long startPosForSynchOp = -1L;
    int adjustment = 0;
    Oplog emptyOplog = null;
    if (DiskStoreImpl.KRF_DEBUG) {
      // wait for cache close to create krf
      System.out.println("basicModify KRF_DEBUG");
      Thread.sleep(1000);
    }
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) {
        // synchronized (this.crf) {
        if (getOplogSet().getChild() != this) {
          useNextOplog = true;
        } else {
          initOpState(OPLOG_MOD_ENTRY_1ID, dr, entry, value, userBits, false);
          adjustment = getOpStateSize();
          assert adjustment > 0;
          long temp = (this.crf.currSize + adjustment);
          if (temp > getMaxCrfSize() && !isFirstRecord()) {
            switchOpLog(dr, adjustment, entry);
            // we can't reuse it since it contains variable length data
            useNextOplog = true;
          } else {
            if (this.lockedForKRFcreate) {
              CacheClosedException cce = new CacheClosedException("The disk store is closed.");
              dr.getCancelCriterion().checkCancelInProgress(cce);
              throw cce;
            }
            this.firstRecord = false;
            long oldOplogId;
            // do the io while holding lock so that switch can set doneAppending
            // Write the data to the opLog for the synch mode
            startPosForSynchOp = writeOpLogBytes(this.crf, async, true);
            this.crf.currSize = temp;
            startPosForSynchOp += getOpStateValueOffset();
            if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
              VersionTag tag = null;
              if (entry.getVersionStamp() != null) {
                tag = entry.getVersionStamp().asVersionTag();
              }
              logger.trace(LogMarker.PERSIST_WRITES,
                  "basicModify: id=<{}> key=<{}> valueOffset={} userBits={} valueLen={} valueBytes=<{}> drId={} versionStamp={} oplog#{}",
                  abs(id.getKeyId()), entry.getKey(), startPosForSynchOp, userBits,
                  value.getLength(), value.getBytesAsString(), dr.getId(), tag, getOplogId());
            }
            if (EntryBits.isNeedsValue(userBits)) {
              id.setValueLength(value.getLength());
            } else {
              id.setValueLength(0);
            }
            id.setUserBits(userBits);
            if (logger.isTraceEnabled()) {
              logger.trace("Oplog::basicModify:Released ByteBuffer with data for Disk ID = {}", id);
            }
            synchronized (id) {
              // Need to do this while synced on id
              // now that we compact forward to most recent oplog.
              // @todo darrel: The sync logic in the disk code is so complex
              // a really doubt is is correct.
              // I think we need to do a fresh rewrite of it.
              oldOplogId = id.setOplogId(getOplogId());
              if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits)) {
                id.setOffsetInOplog(-1);
              } else {
                id.setOffsetInOplog(startPosForSynchOp);
              }
            }
            // Set the oplog size change for stats
            this.dirHolder.incrementTotalOplogSize(adjustment);
            this.incTotalCount();

            EntryLogger.logPersistPut(dr.getName(), entry.getKey(), dr.getDiskStoreID());
            if (oldOplogId != getOplogId()) {
              Oplog oldOplog = getOplogSet().getChild(oldOplogId);
              if (oldOplog != null) {
                oldOplog.rmLive(dr, entry);
                emptyOplog = oldOplog;
              }
              addLive(dr, entry);
              // Note if this mod was done to oldOplog then this entry is already
              // in
              // the linked list. All we needed to do in this case is call
              // incTotalCount
            } else {
              getOrCreateDRI(dr).update(entry);
            }

            // Update the region version vector for the disk store.
            // This needs to be done under lock so that we don't switch oplogs
            // unit the version vector accurately represents what is in this oplog
            RegionVersionVector rvv = dr.getRegionVersionVector();
            if (rvv != null && entry.getVersionStamp() != null) {
              rvv.recordVersion(entry.getVersionStamp().getMemberID(),
                  entry.getVersionStamp().getRegionVersion());
            }
          }
          clearOpState();
        }
        // }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(getOplogSet().getChild() != this);
      getOplogSet().getChild().basicModify(dr, entry, value, userBits, async, calledByCompactor);
    } else {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSettingOplogOffSet(startPosForSynchOp);
      }
      if (emptyOplog != null
          && (!emptyOplog.isCompacting() || emptyOplog.calledByCompactorThread())) {
        if (calledByCompactor && emptyOplog.hasNoLiveValues()) {
          // Since compactor will only append to crf no need to flush drf.
          // Before we have the compactor delete an oplog it has emptied out
          // we want to have it flush anything it has written to the current
          // oplog.
          // Note that since sync writes may be done to the same oplog we are
          // doing
          // async writes to any sync writes will cause a flush to be done
          // immediately.
          flushAll(true);
        }
        emptyOplog.handleNoLiveValues();
      }
    }
  }

  private void basicSaveConflictVersionTag(DiskRegionView dr, VersionTag tag, boolean async)
      throws IOException, InterruptedException {
    boolean useNextOplog = false;
    int adjustment = 0;
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) {
        if (getOplogSet().getChild() != this) {
          useNextOplog = true;
        } else {
          this.opState.initialize(OPLOG_CONFLICT_VERSION, dr.getId(), tag);
          adjustment = getOpStateSize();
          assert adjustment > 0;
          long temp = (this.crf.currSize + adjustment);
          if (temp > getMaxCrfSize() && !isFirstRecord()) {
            switchOpLog(dr, adjustment, null);
            // we can't reuse it since it contains variable length data
            useNextOplog = true;
          } else {
            if (this.lockedForKRFcreate) {
              CacheClosedException cce = new CacheClosedException("The disk store is closed.");
              dr.getCancelCriterion().checkCancelInProgress(cce);
              throw cce;
            }
            this.firstRecord = false;
            writeOpLogBytes(this.crf, async, true);
            this.crf.currSize = temp;
            if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
              logger.trace(LogMarker.PERSIST_WRITES,
                  "basicSaveConflictVersionTag: drId={} versionStamp={} oplog#{}", dr.getId(), tag,
                  getOplogId());
            }
            this.dirHolder.incrementTotalOplogSize(adjustment);
            // Update the region version vector for the disk store.
            // This needs to be done under lock so that we don't switch oplogs
            // unit the version vector accurately represents what is in this oplog
            RegionVersionVector rvv = dr.getRegionVersionVector();
            if (rvv != null && dr.getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
              rvv.recordVersion(tag.getMemberID(), tag.getRegionVersion());
            }
          }
          clearOpState();
        }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(getOplogSet().getChild() != this);
      getOplogSet().getChild().basicSaveConflictVersionTag(dr, tag, async);
    }
  }

  private void basicCopyForwardForOfflineCompact(long oplogKeyId, byte[] keyBytes,
      byte[] valueBytes, byte userBits, long drId, VersionTag tag)
      throws IOException, InterruptedException {
    boolean useNextOplog = false;
    long startPosForSynchOp = -1L;
    int adjustment = 0;
    // No need to get the backup lock since this is only for offline compaction
    synchronized (this.lock) {
      // synchronized (this.crf) {
      if (getOplogSet().getChild() != this) {
        useNextOplog = true;
      } else {
        this.opState.initialize(oplogKeyId, keyBytes, valueBytes, userBits, drId, tag, false);
        adjustment = getOpStateSize();
        assert adjustment > 0;
        long temp = (this.crf.currSize + adjustment);
        if (temp > getMaxCrfSize() && !isFirstRecord()) {
          switchOpLog(null, adjustment, null);
          // we can't reuse it since it contains variable length data
          useNextOplog = true;
        } else {
          this.firstRecord = false;
          // do the io while holding lock so that switch can set doneAppending
          // Write the data to the opLog async since we are offline compacting
          startPosForSynchOp = writeOpLogBytes(this.crf, true, true);
          this.crf.currSize = temp;
          startPosForSynchOp += getOpStateValueOffset();
          getOplogSet().getChild().writeOneKeyEntryForKRF(keyBytes, userBits, valueBytes.length,
              drId, oplogKeyId, startPosForSynchOp, tag);

          if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
            logger.trace(LogMarker.PERSIST_WRITES,
                "basicCopyForwardForOfflineCompact: id=<{}> keyBytes=<{}> valueOffset={} userBits={} valueLen={} valueBytes=<{}> drId={} oplog#{}",
                oplogKeyId, baToString(keyBytes), startPosForSynchOp, userBits, valueBytes.length,
                baToString(valueBytes), drId, getOplogId());
          }

          this.dirHolder.incrementTotalOplogSize(adjustment);
          this.incTotalCount();
        }
        clearOpState();
      }
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(getOplogSet().getChild() != this);
      getOplogSet().getChild().basicCopyForwardForOfflineCompact(oplogKeyId, keyBytes, valueBytes,
          userBits, drId, tag);
    }
  }

  private boolean isCompacting() {
    return this.compacting;
  }

  private void addLive(DiskRegionView dr, DiskEntry de) {
    getOrCreateDRI(dr).addLive(de);
    incLiveCount();
  }

  private void rmLive(DiskRegionView dr, DiskEntry de) {
    DiskRegionInfo dri = getOrCreateDRI(dr);
    synchronized (dri) {
      dri.rmLive(de, this);
    }
  }

  private DiskRegionInfo getDRI(long drId) {
    return this.regionMap.get(drId);
  }

  private DiskRegionInfo getDRI(DiskRegionView dr) {
    return getDRI(dr.getId());
  }

  private DiskRegionInfo getOrCreateDRI(DiskRegionView dr) {
    DiskRegionInfo dri = getDRI(dr);
    if (dri == null) {
      dri = (isCompactionPossible() || couldHaveKrf())
          ? new DiskRegionInfoWithList(dr, couldHaveKrf(), this.krfCreated.get())
          : new DiskRegionInfoNoList(dr);
      DiskRegionInfo oldDri = this.regionMap.putIfAbsent(dr.getId(), dri);
      if (oldDri != null) {
        dri = oldDri;
      }
    }
    return dri;
  }

  public boolean needsKrf() {
    return couldHaveKrf() && !krfCreated.get();
  }

  /**
   * @return true if this Oplog could end up having a KRF file.
   */
  private boolean couldHaveKrf() {
    return getOplogSet().couldHaveKrf();
  }

  private DiskRegionInfo getOrCreateDRI(long drId) {
    DiskRegionInfo dri = getDRI(drId);
    if (dri == null) {
      dri = (isCompactionPossible() || couldHaveKrf())
          ? new DiskRegionInfoWithList(null, couldHaveKrf(), this.krfCreated.get())
          : new DiskRegionInfoNoList(null);
      DiskRegionInfo oldDri = this.regionMap.putIfAbsent(drId, dri);
      if (oldDri != null) {
        dri = oldDri;
      }
    }
    return dri;
  }

  /**
   * Removes the key/value pair with the given id on disk.
   * 
   * @param entry DiskEntry object on which remove operation is called
   */
  public void remove(LocalRegion region, DiskEntry entry, boolean async, boolean isClear) {
    DiskRegion dr = region.getDiskRegion();
    if (getOplogSet().getChild() != this) {
      getOplogSet().getChild().remove(region, entry, async, isClear);
    } else {
      DiskId did = entry.getDiskId();
      boolean exceptionOccurred = false;
      byte prevUsrBit = did.getUserBits();
      int len = did.getValueLength();
      try {
        basicRemove(dr, entry, async, isClear);
      } catch (IOException ex) {
        exceptionOccurred = true;
        getParent().getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0
            .toLocalizedString(this.diskFile.getPath()), ex, dr.getName());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        region.getCancelCriterion().checkCancelInProgress(ie);
        exceptionOccurred = true;
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING
                .toLocalizedString(this.diskFile.getPath()),
            ie, dr.getName());
      } finally {
        if (exceptionOccurred) {
          did.setValueLength(len);
          did.setUserBits(prevUsrBit);
        }
      }
    }
  }

  /**
   * Write the GC RVV for a single region to disk
   */
  public void writeGCRVV(DiskRegion dr) {
    boolean useNextOplog = false;
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) {
        if (getOplogSet().getChild() != this) {
          useNextOplog = true;
        } else {
          try {
            writeRVVRecord(this.drf,
                Collections.<Long, AbstractDiskRegion>singletonMap(dr.getId(), dr), true);
          } catch (IOException ex) {
            dr.getCancelCriterion().checkCancelInProgress(ex);
            throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_RECORDING_RVV_BECAUSE_OF_0
                .toLocalizedString(this.diskFile.getPath()), ex, dr.getName());
          }
        }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      getOplogSet().getChild().writeGCRVV(dr);
    } else {
      DiskStoreObserver.endWriteGCRVV(dr);
    }
  }

  /**
   * There're 3 cases to use writeRVV: 1) endGII: DiskRegion.writeRVV(region=null, true),
   * Oplog.writeRVV(true,null) 2) beginGII: DiskRegion.writeRVV(region=this, false),
   * Oplog.writeRVV(false,sourceRVV!=null) 3) clear: DiskRegion.writeRVV(region=this, null),
   * Oplog.writeRVV(null,sourceRVV!=null)
   */
  public void writeRVV(DiskRegion dr, RegionVersionVector sourceRVV, Boolean isRVVTrusted) {
    boolean useNextOplog = false;
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) {
        if (getOplogSet().getChild() != this) {
          useNextOplog = true;
        } else {

          try {
            // We'll update the RVV of the disk region while holding the lock on
            // the oplog,
            // to make sure we don't switch oplogs while we're in the middle of
            // this.
            if (sourceRVV != null) {
              dr.getRegionVersionVector().recordVersions(sourceRVV);
            } else {
              // it's original EndGII, not to write duplicate rvv if its trusted
              if (dr.getRVVTrusted()) {
                return;
              }
            }
            if (isRVVTrusted != null) {
              // isRVVTrusted == null means "as is"
              dr.setRVVTrusted(isRVVTrusted);
            }
            writeRVVRecord(this.crf,
                Collections.<Long, AbstractDiskRegion>singletonMap(dr.getId(), dr), false);
          } catch (IOException ex) {
            dr.getCancelCriterion().checkCancelInProgress(ex);
            throw new DiskAccessException(LocalizedStrings.Oplog_FAILED_RECORDING_RVV_BECAUSE_OF_0
                .toLocalizedString(this.diskFile.getPath()), ex, dr.getName());
          }
        }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      getOplogSet().getChild().writeRVV(dr, sourceRVV, isRVVTrusted);
    }
  }

  private long getMaxCrfSize() {
    return this.maxCrfSize;
  }

  private long getMaxDrfSize() {
    return this.maxDrfSize;
  }

  private void setMaxCrfDrfSize() {
    int crfPct = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "CRF_MAX_PERCENTAGE", 90);
    if (crfPct > 100 || crfPct < 0) {
      crfPct = 90;
    }
    this.maxCrfSize = (long) (this.maxOplogSize * (crfPct / 100.0));
    this.maxDrfSize = this.maxOplogSize - this.maxCrfSize;
  }

  /**
   * A helper function which identifies whether to record a removal of entry in the current oplog or
   * to make the switch to the next oplog. This function enables us to reuse the byte buffer which
   * got created for an oplog which no longer permits us to use itself. It will also take acre of
   * compaction if required
   * 
   * @param entry DiskEntry object representing the current Entry
   */
  private void basicRemove(DiskRegionView dr, DiskEntry entry, boolean async, boolean isClear)
      throws IOException, InterruptedException {
    DiskId id = entry.getDiskId();

    boolean useNextOplog = false;
    long startPosForSynchOp = -1;
    Oplog emptyOplog = null;
    if (DiskStoreImpl.KRF_DEBUG) {
      // wait for cache close to create krf
      System.out.println("basicRemove KRF_DEBUG");
      Thread.sleep(1000);
    }
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock) {
        if (getOplogSet().getChild() != this) {
          useNextOplog = true;
        } else if ((this.drf.currSize + MAX_DELETE_ENTRY_RECORD_BYTES) > getMaxDrfSize()
            && !isFirstRecord()) {
          switchOpLog(dr, MAX_DELETE_ENTRY_RECORD_BYTES, entry);
          useNextOplog = true;
        } else {
          if (this.lockedForKRFcreate) {
            CacheClosedException cce = new CacheClosedException("The disk store is closed.");
            dr.getCancelCriterion().checkCancelInProgress(cce);
            throw cce;
          }
          long oldOplogId = id.setOplogId(getOplogId());
          if (!isClear) {
            this.firstRecord = false;
            // Ok now we can go ahead and find out its actual size
            // This is the only place to set notToUseUserBits=true
            initOpState(OPLOG_DEL_ENTRY_1ID, dr, entry, null, (byte) 0, true);
            int adjustment = getOpStateSize();

            this.drf.currSize += adjustment;
            // do the io while holding lock so that switch can set doneAppending
            if (logger.isTraceEnabled()) {
              logger.trace(
                  "Oplog::basicRemove: Recording the Deletion of entry in the Oplog with id = {} The Oplog Disk ID for the entry being deleted = {} Mode is Synch",
                  getOplogId(), id);
            }

            // Write the data to the opLog for the synch mode
            // TODO: if we don't sync write destroys what will happen if
            // we do 1. create k1 2. destroy k1 3. create k1?
            // It would be possible for the crf to be flushed but not the drf.
            // Then during recovery we will find identical keys with different
            // entryIds.
            // I think we can safely have drf writes be async as long as we flush
            // the drf
            // before we flush the crf.
            // However we can't have removes by async if we are doing a sync write
            // because we might be killed right after we do this write.
            startPosForSynchOp = writeOpLogBytes(this.drf, async, true);
            setHasDeletes(true);
            if (logger.isDebugEnabled(LogMarker.PERSIST_WRITES)) {
              logger.debug("basicRemove: id=<{}> key=<{}> drId={} oplog#{}", abs(id.getKeyId()),
                  entry.getKey(), dr.getId(), getOplogId());
            }

            if (logger.isTraceEnabled()) {
              logger.trace("Oplog::basicRemove:Released ByteBuffer for Disk ID = {}", id);
            }
            this.dirHolder.incrementTotalOplogSize(adjustment);
          }
          // Set the oplog size change for stats
          id.setOffsetInOplog(-1);

          EntryLogger.logPersistDestroy(dr.getName(), entry.getKey(), dr.getDiskStoreID());
          Oplog rmOplog = null;
          if (oldOplogId == getOplogId()) {
            rmOplog = this;
          } else {
            rmOplog = getOplogSet().getChild(oldOplogId);
          }
          if (rmOplog != null) {
            rmOplog.rmLive(dr, entry);
            emptyOplog = rmOplog;
          }
          clearOpState();
        }
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    if (useNextOplog) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSwitchingOplog();
      }
      Assert.assertTrue(getOplogSet().getChild() != this);
      getOplogSet().getChild().basicRemove(dr, entry, async, isClear);
    } else {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterSettingOplogOffSet(startPosForSynchOp);
      }
      if (emptyOplog != null
          && (!emptyOplog.isCompacting() || emptyOplog.calledByCompactorThread())) {
        emptyOplog.handleNoLiveValues();
      }
    }
  }

  /**
   * test hook
   */
  ByteBuffer getWriteBuf() {
    return this.crf.writeBuf;
  }

  private void flushNoSync(OplogFile olf) throws IOException {
    flushAllNoSync(false); // @todo
  }

  @Override
  public void flush() throws IOException {
    flushAllNoSync(false);
  }

  @Override
  public void flush(ByteBuffer b1, ByteBuffer b2) throws IOException {
    if (b1 == this.drf.writeBuf) {
      flush(this.drf, b1, b2);
      flush(this.crf, false);
    } else {
      flush(this.drf, false);
      flush(this.crf, b1, b2);
    }
  }

  private void flushAndSync(OplogFile olf) throws IOException {
    flushAll(false);
  }

  private static final int MAX_CHANNEL_RETRIES = 5;

  private void flush(OplogFile olf, boolean doSync) throws IOException {
    try {
      // No need to get the backup lock prior to synchronizing (correct lock order) since the
      // synchronized block does not attempt to get the backup lock (incorrect lock order)
      synchronized (this.lock/* olf */) {
        if (olf.RAFClosed) {
          return;
        }
        ByteBuffer bb = olf.writeBuf;
        if (bb != null && bb.position() != 0) {
          bb.flip();
          int flushed = 0;
          int numChannelRetries = 0;
          do {
            int channelBytesWritten = 0;
            final int bbStartPos = bb.position();
            final long channelStartPos = olf.channel.position();
            // differentiate between bytes written on this channel.write() iteration and the
            // total number of bytes written to the channel on this call
            channelBytesWritten = olf.channel.write(bb);
            // Expect channelBytesWritten and the changes in pp.position() and channel.position() to
            // be the same. If they are not, then the channel.write() silently failed. The following
            // retry separates spurious failures from permanent channel failures.
            if (channelBytesWritten != bb.position() - bbStartPos) {
              if (numChannelRetries++ < MAX_CHANNEL_RETRIES) {
                // Reset the ByteBuffer position, but take into account anything that did get
                // written to the channel
                channelBytesWritten = (int) (olf.channel.position() - channelStartPos);
                bb.position(bbStartPos + channelBytesWritten);
              } else {
                throw new IOException("Failed to write Oplog entry to" + olf.f.getName() + ": "
                    + "channel.write() returned " + channelBytesWritten + ", "
                    + "change in channel position = " + (olf.channel.position() - channelStartPos)
                    + ", " + "change in source buffer position = " + (bb.position() - bbStartPos));
              }
            }
            flushed += channelBytesWritten;
          } while (bb.hasRemaining());
          // update bytesFlushed after entire writeBuffer is flushed to fix bug
          // 41201
          olf.bytesFlushed += flushed;
          bb.clear();
        }
        if (doSync) {
          if (SYNC_WRITES) {
            // Synch Meta Data as well as content
            olf.channel.force(true);
          }
        }
      }
    } catch (ClosedChannelException ignore) {
      // It is possible for a channel to be closed when our code does not
      // explicitly call channel.close (when we will set RAFclosed).
      // This can happen when a thread is doing an io op and is interrupted.
      // That thread will see ClosedByInterruptException but it will also
      // close the channel and then we will see ClosedChannelException.
    }
  }

  private void flush(OplogFile olf, ByteBuffer b1, ByteBuffer b2) throws IOException {
    try {
      // No need to get the backup lock prior to synchronizing (correct lock order) since the
      // synchronized block does not attempt to get the backup lock (incorrect lock order)
      synchronized (this.lock/* olf */) {
        if (olf.RAFClosed) {
          return;
        }
        this.bbArray[0] = b1;
        this.bbArray[1] = b2;
        b1.flip();
        long flushed = 0;
        do {
          flushed += olf.channel.write(this.bbArray);
        } while (b2.hasRemaining());
        this.bbArray[0] = null;
        this.bbArray[1] = null;
        // update bytesFlushed after entire writeBuffer is flushed to fix bug 41201
        olf.bytesFlushed += flushed;
        b1.clear();
      }
    } catch (ClosedChannelException ignore) {
      // It is possible for a channel to be closed when our code does not
      // explicitly call channel.close (when we will set RAFclosed).
      // This can happen when a thread is doing an io op and is interrupted.
      // That thread will see ClosedByInterruptException but it will also
      // close the channel and then we will see ClosedChannelException.
    }
  }

  public void flushAll() {
    flushAll(false);
  }

  public void flushAllNoSync(boolean skipDrf) {
    flushAll(skipDrf, false);
  }

  public void flushAll(boolean skipDrf) {
    flushAll(skipDrf, true/* doSync */);
  }

  public void flushAll(boolean skipDrf, boolean doSync) {
    try {
      // TODO: if skipDrf then only need to do drf if crf has flushable data
      flush(this.drf, doSync);
      flush(this.crf, doSync);
    } catch (IOException ex) {
      getParent().getCancelCriterion().checkCancelInProgress(ex);
      throw new DiskAccessException(
          LocalizedStrings.Oplog_FAILED_WRITING_KEY_TO_0.toLocalizedString(this.diskFile.getPath()),
          ex, getParent());
    }
  }

  /**
   * Since the ByteBuffer being writen to can have additional bytes which are used for extending the
   * size of the file, it is necessary that the ByteBuffer provided should have limit which is set
   * to the position till which it contains the actual bytes. If the mode is synched write then only
   * we will write up to the capacity & opLogSpace variable have any meaning. For asynch mode it
   * will be zero. Also this method must be synchronized on the file , whether we use synch or
   * asynch write because the fault in operations can clash with the asynch writing. Write the
   * specified bytes to the oplog. Note that since extending a file is expensive this code will
   * possibly write OPLOG_EXTEND_SIZE zero bytes to reduce the number of times the file is extended.
   * 
   *
   * @param olf the file to write the bytes to
   * @return The long offset at which the data present in the ByteBuffer gets written to
   */
  private long writeOpLogBytes(OplogFile olf, boolean async, boolean doFlushIfSync)
      throws IOException {
    long startPos = -1L;
    getParent().getBackupLock().lock();
    try {
      synchronized (this.lock/* olf */) {
        Assert.assertTrue(!this.doneAppending);
        if (this.closed) {
          Assert.assertTrue(false, "The Oplog " + this.oplogId + " for store "
              + getParent().getName()
              + " has been closed for synch mode while writing is going on. This should not happen");
        }
        // It is assumed that the file pointer is already at the
        // appropriate position in the file so as to allow writing at the end.
        // Any fault in operations will set the pointer back to the write
        // location.
        // Also it is only in case of synch writing, we are writing more
        // than what is actually needed, we will have to reset the pointer.
        // Also need to add in offset in writeBuf in case we are not flushing
        // writeBuf
        startPos = olf.channel.position() + olf.writeBuf.position();
        // Assert.assertTrue(startPos > lastWritePos,
        // "startPos=" + startPos +
        // " was not > lastWritePos=" + lastWritePos);
        long bytesWritten = this.opState.write(olf);
        if (!async && doFlushIfSync) {
          flushAndSync(olf);
        }
        getStats().incWrittenBytes(bytesWritten, async);

        // // Moved the set of lastWritePos to after write
        // // so if write throws an exception it will not be updated.
        // // This fixes bug 40449.
        // this.lastWritePos = startPos;
      }
    } finally {
      getParent().getBackupLock().unlock();
    }
    return startPos;
  }

  boolean isRAFOpen() {
    return !this.crf.RAFClosed; // volatile read
  }

  private boolean okToReopen;

  boolean closeRAF() {
    if (this.beingRead)
      return false;
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* crf */) {
      if (this.beingRead)
        return false;
      if (!this.doneAppending)
        return false;
      if (this.crf.RAFClosed) {
        return false;
      } else {
        try {
          this.crf.raf.close();
        } catch (IOException ignore) {
        }
        this.crf.RAFClosed = true;
        this.okToReopen = true;
        this.stats.decOpenOplogs();
        return true;
      }
    }
  }

  private volatile boolean beingRead;

  /**
   * If crfRAF has been closed then attempt to reopen the oplog for this read. Verify that this only
   * happens when test methods are invoked.
   * 
   * @return true if oplog file is open and can be read from; false if not
   */
  private boolean reopenFileIfClosed() throws IOException {
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* crf */) {
      boolean result = !this.crf.RAFClosed;
      if (!result && this.okToReopen) {
        result = true;
        this.crf.raf = new UninterruptibleRandomAccessFile(this.crf.f, "r");
        this.stats.incOpenOplogs();
        this.crf.RAFClosed = false;
        this.okToReopen = false;
      }
      return result;
    }
  }

  private BytesAndBits attemptGet(DiskRegionView dr, long offsetInOplog, boolean bitOnly,
      int valueLength, byte userBits) throws IOException {
    boolean didReopen = false;
    boolean accessedInactive = false;
    try {
      // No need to get the backup lock prior to synchronizing (correct lock order) since the
      // synchronized block does not attempt to get the backup lock (incorrect lock order)
      synchronized (this.lock/* crf */) {
        // if (this.closed || this.deleted.get()) {
        // throw new DiskAccessException("attempting get on "
        // + (this.deleted.get() ? "destroyed" : "closed")
        // + " oplog #" + getOplogId(), this.owner);
        // }
        this.beingRead = true;
        if (/*
             * !getParent().isSync() since compactor groups writes &&
             */(offsetInOplog + valueLength) > this.crf.bytesFlushed && !this.closed) {
          flushAllNoSync(true); // fix for bug 41205
        }
        try {
          UninterruptibleRandomAccessFile myRAF = null;
          if (this.crf.RAFClosed) {
            myRAF = new UninterruptibleRandomAccessFile(this.crf.f, "r");
            this.stats.incOpenOplogs();
            if (this.okToReopen) {
              this.crf.RAFClosed = false;
              this.okToReopen = false;
              this.crf.raf = myRAF;
              didReopen = true;
            }
          } else {
            myRAF = this.crf.raf;
            accessedInactive = true;
          }
          BytesAndBits bb = null;
          try {
            final long writePosition =
                (this.doneAppending) ? this.crf.bytesFlushed : myRAF.getFilePointer();
            if ((offsetInOplog + valueLength) > writePosition) {
              throw new DiskAccessException(
                  LocalizedStrings.Oplog_TRIED_TO_SEEK_TO_0_BUT_THE_FILE_LENGTH_IS_1_OPLOG_FILE_OBJECT_USED_FOR_READING_2
                      .toLocalizedString(offsetInOplog + valueLength, writePosition, this.crf.raf),
                  dr.getName());
            } else if (offsetInOplog < 0) {
              throw new DiskAccessException(
                  LocalizedStrings.Oplog_CANNOT_FIND_RECORD_0_WHEN_READING_FROM_1
                      .toLocalizedString(offsetInOplog, this.diskFile.getPath()),
                  dr.getName());
            }
            try {
              myRAF.seek(offsetInOplog);
              this.stats.incOplogSeeks();
              byte[] valueBytes = new byte[valueLength];
              myRAF.readFully(valueBytes);
              this.stats.incOplogReads();
              bb = new BytesAndBits(valueBytes, userBits);
              // also set the product version for an older product
              final Version version = getProductVersionIfOld();
              if (version != null) {
                bb.setVersion(version);
              }
            } finally {
              // if this oplog is no longer being appended to then don't waste
              // disk io
              if (!this.doneAppending) {
                // by seeking back to writePosition
                myRAF.seek(writePosition);
                this.stats.incOplogSeeks();
              }
            }
            return bb;
          } finally {
            if (myRAF != this.crf.raf) {
              try {
                myRAF.close();
              } catch (IOException ignore) {
              }
            }
          }
        } finally {
          this.beingRead = false;
          // if (this.closed || this.deleted.get()) {
          // throw new DiskAccessException("attempting get on "
          // + (this.deleted.get() ? "destroyed" : "closed")
          // + " oplog #" + getOplogId(), this.owner);
          // }
        }
      } // sync
    } finally {
      if (accessedInactive) {
        getOplogSet().inactiveAccessed(this);
      } else if (didReopen) {
        getOplogSet().inactiveReopened(this);
      }
    }
  }

  /**
   * Extracts the Value byte array & UserBit from the OpLog
   * 
   * @param offsetInOplog The starting position from which to read the data in the opLog
   * @param bitOnly boolean indicating whether the value needs to be extracted along with the
   *        UserBit or not.
   * @param valueLength The length of the byte array which represents the value
   * @param userBits The userBits of the value.
   * @return BytesAndBits object which wraps the extracted value & user bit
   */
  private BytesAndBits basicGet(DiskRegionView dr, long offsetInOplog, boolean bitOnly,
      int valueLength, byte userBits) {
    BytesAndBits bb = null;
    if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits) || bitOnly
        || valueLength == 0) {
      if (EntryBits.isInvalid(userBits)) {
        bb = new BytesAndBits(DiskEntry.INVALID_BYTES, userBits);
      } else if (EntryBits.isTombstone(userBits)) {
        bb = new BytesAndBits(DiskEntry.TOMBSTONE_BYTES, userBits);
      } else {
        bb = new BytesAndBits(DiskEntry.LOCAL_INVALID_BYTES, userBits);
      }
    } else {
      if (offsetInOplog == -1)
        return null;
      try {
        for (;;) {
          dr.getCancelCriterion().checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            bb = attemptGet(dr, offsetInOplog, bitOnly, valueLength, userBits);
            break;
          } catch (InterruptedIOException ignore) { // bug 39756
            // ignore, we'll clear and retry.
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      } catch (IOException ex) {
        getParent().getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_READING_FROM_0_OPLOGID_1_OFFSET_BEING_READ_2_CURRENT_OPLOG_SIZE_3_ACTUAL_FILE_SIZE_4_IS_ASYNCH_MODE_5_IS_ASYNCH_WRITER_ALIVE_6
                .toLocalizedString(this.diskFile.getPath(), this.oplogId, offsetInOplog,
                    this.crf.currSize, this.crf.bytesFlushed, !dr.isSync(), Boolean.FALSE),
            ex, dr.getName());
      } catch (IllegalStateException ex) {
        checkClosed();
        throw ex;
      }
    }
    return bb;
  }

  /**
   * Extracts the Value byte array & UserBit from the OpLog and inserts it in the wrapper Object of
   * type BytesAndBitsForCompactor which is passed
   * 
   * @param offsetInOplog The starting position from which to read the data in the opLog
   * @param bitOnly boolean indicating whether the value needs to be extracted along with the
   *        UserBit or not.
   * @param valueLength The length of the byte array which represents the value
   * @param userBits The userBits of the value.
   * @param wrapper Object of type BytesAndBitsForCompactor. The data is set in the wrapper Object.
   *        The wrapper Object also contains the user bit associated with the entry
   * @return true if data is found false if not
   */
  private boolean basicGetForCompactor(DiskRegionView dr, long offsetInOplog, boolean bitOnly,
      int valueLength, byte userBits, BytesAndBitsForCompactor wrapper) {
    if (EntryBits.isAnyInvalid(userBits) || EntryBits.isTombstone(userBits) || bitOnly
        || valueLength == 0) {
      if (EntryBits.isInvalid(userBits)) {
        wrapper.setData(DiskEntry.INVALID_BYTES, userBits, DiskEntry.INVALID_BYTES.length,
            false /*
                   * Cannot be reused
                   */);
      } else if (EntryBits.isTombstone(userBits)) {
        wrapper.setData(DiskEntry.TOMBSTONE_BYTES, userBits, DiskEntry.TOMBSTONE_BYTES.length,
            false /*
                   * Cannot be reused
                   */);
      } else {
        wrapper.setData(DiskEntry.LOCAL_INVALID_BYTES, userBits,
            DiskEntry.LOCAL_INVALID_BYTES.length, false /*
                                                         * Cannot be reused
                                                         */);
      }
    } else {
      try {
        // No need to get the backup lock prior to synchronizing (correct lock order) since the
        // synchronized block does not attempt to get the backup lock (incorrect lock order)
        synchronized (this.lock/* crf */) {
          if (/*
               * !getParent().isSync() since compactor groups writes &&
               */(offsetInOplog + valueLength) > this.crf.bytesFlushed && !this.closed) {
            flushAllNoSync(true); // fix for bug 41205
          }
          if (!reopenFileIfClosed()) {
            return false; // fix for bug 40648
          }
          final long writePosition =
              (this.doneAppending) ? this.crf.bytesFlushed : this.crf.raf.getFilePointer();
          if ((offsetInOplog + valueLength) > writePosition) {
            throw new DiskAccessException(
                LocalizedStrings.Oplog_TRIED_TO_SEEK_TO_0_BUT_THE_FILE_LENGTH_IS_1_OPLOG_FILE_OBJECT_USED_FOR_READING_2
                    .toLocalizedString(offsetInOplog + valueLength, writePosition, this.crf.raf),
                dr.getName());
          } else if (offsetInOplog < 0) {
            throw new DiskAccessException(
                LocalizedStrings.Oplog_CANNOT_FIND_RECORD_0_WHEN_READING_FROM_1
                    .toLocalizedString(offsetInOplog, this.diskFile.getPath()),
                dr.getName());
          }
          try {
            this.crf.raf.seek(offsetInOplog);
            this.stats.incOplogSeeks();
            byte[] valueBytes = null;
            if (wrapper.getBytes().length < valueLength) {
              valueBytes = new byte[valueLength];
              this.crf.raf.readFully(valueBytes);
            } else {
              valueBytes = wrapper.getBytes();
              this.crf.raf.readFully(valueBytes, 0, valueLength);
            }
            this.stats.incOplogReads();
            wrapper.setData(valueBytes, userBits, valueLength, true);
          } finally {
            // if this oplog is no longer being appended to then don't waste
            // disk io
            if (!this.doneAppending) {
              this.crf.raf.seek(writePosition);
              this.stats.incOplogSeeks();
            }
            // if (this.closed || this.deleted.get()) {
            // throw new DiskAccessException("attempting get on "
            // + (this.deleted.get() ? "destroyed" : "closed")
            // + " oplog #" + getOplogId(), this.owner);
            // }
          }
        }
      } catch (IOException ex) {
        getParent().getCancelCriterion().checkCancelInProgress(ex);
        throw new DiskAccessException(
            LocalizedStrings.Oplog_FAILED_READING_FROM_0_OPLOG_DETAILS_1_2_3_4_5_6
                .toLocalizedString(this.diskFile.getPath(), this.oplogId, offsetInOplog,
                    this.crf.currSize, this.crf.bytesFlushed, Boolean.FALSE, Boolean.FALSE),
            ex, dr.getName());

      } catch (IllegalStateException ex) {
        checkClosed();
        throw ex;
      }
    }
    return true;
  }

  private final AtomicBoolean deleted = new AtomicBoolean();

  /**
   * deletes the oplog's file(s)
   */
  void deleteFiles(boolean crfOnly) {
    // try doing the removeOplog unconditionally since I'm see an infinite loop
    // in destroyOldestReadyToCompact
    boolean needsDestroy = this.deleted.compareAndSet(false, true);

    if (needsDestroy) {
      // I don't under stand why the compactor would have anything to do with
      // an oplog file that we are removing from disk.
      // So I'm commenting out the following if
      // if (!isCompactionPossible()) {
      // moved this from close to fix bug 40574
      // If we get to the point that it is ok to close the file
      // then we no longer need the parent to be able to find this
      // oplog using its id so we can unregister it now.
      // If compaction is possible then we need to leave this
      // oplog registered with the parent and allow the compactor to unregister
      // it.
      // }

      deleteCRF();
      if (!crfOnly || !getHasDeletes()) {
        setHasDeletes(false);
        deleteDRF();
        // no need to call removeDrf since parent removeOplog did it
        // getParent().removeDrf(this);
        // getParent().oplogSetRemove(this);
      }

      // Fix for bug 42495 - Don't remove the oplog from this list
      // of oplogs until it has been removed from the init file. This guarantees
      // that if the oplog is in the init file, the backup code can find it and
      // try to back it up.
      boolean addToDrfOnly = crfOnly && getHasDeletes();
      getOplogSet().removeOplog(getOplogId(), true, addToDrfOnly ? this : null);
    } else if (!crfOnly && getHasDeletes()) {
      setHasDeletes(false);
      deleteDRF();
      getOplogSet().removeDrf(this);
      // getParent().oplogSetRemove(this);
    }

  }

  public void deleteCRF() {
    oplogSet.crfDelete(this.oplogId);
    BackupManager backupManager = getInternalCache().getBackupManager();
    DiskStoreBackup inProgressBackup = getParent().getInProgressBackup();
    if (inProgressBackup == null || !inProgressBackup.deferCrfDelete(this)) {
      deleteCRFFileOnly();
    }
  }

  public void deleteCRFFileOnly() {
    deleteFile(this.crf);
    // replace .crf at the end with .krf
    if (this.crf.f != null) {
      final File krf = new File(
          this.crf.f.getAbsolutePath().replaceFirst("\\" + CRF_FILE_EXT + "$", KRF_FILE_EXT));
      if (!krf.exists()) {
        return;
      }
      getParent().executeDelayedExpensiveWrite(new Runnable() {
        public void run() {
          if (!krf.delete()) {
            if (krf.exists()) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.Oplog_DELETE_FAIL_0_1_2,
                  new Object[] {Oplog.this.toString(), "krf", getParent().getName()}));
            }
          } else {
            logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_DELETE_0_1_2,
                new Object[] {Oplog.this.toString(), "krf", getParent().getName()}));
          }
        }
      });
    }
  }

  public void deleteDRF() {
    getOplogSet().drfDelete(this.oplogId);
    DiskStoreBackup inProgressBackup = getParent().getInProgressBackup();
    if (inProgressBackup == null || !inProgressBackup.deferDrfDelete(this)) {
      deleteDRFFileOnly();
    }
  }

  public void deleteDRFFileOnly() {
    deleteFile(this.drf);
  }

  /**
   * Returns "crf" or "drf".
   */
  private static String getFileType(OplogFile olf) {
    String name = olf.f.getName();
    int index = name.lastIndexOf('.');
    return name.substring(index + 1);
  }

  private void deleteFile(final OplogFile olf) {
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock) {
      if (olf.currSize != 0) {
        this.dirHolder.decrementTotalOplogSize(olf.currSize);
        olf.currSize = 0;
      }
      if (olf.f == null)
        return;
      if (!olf.f.exists())
        return;
      assert olf.RAFClosed;
      if (!olf.RAFClosed || olf.raf != null) {
        try {
          olf.raf.close();
          olf.RAFClosed = true;
        } catch (IOException ignore) {
        }
      }

      // Delete the file asynchronously. Based on perf testing, deletes
      // can block at the filesystem level. See #50254
      // It's safe to do this asynchronously, because we have already
      // marked this file as deleted in the init file.
      // Note - could we run out of disk space because the compaction thread is
      // doing this and creating files? For a real fix, you probably need a
      // bounded
      // queue
      getParent().executeDelayedExpensiveWrite(new Runnable() {
        public void run() {
          if (!olf.f.delete() && olf.f.exists()) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.Oplog_DELETE_FAIL_0_1_2,
                new Object[] {Oplog.this.toString(), getFileType(olf), getParent().getName()}));
          } else {
            logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_DELETE_0_1_2,
                new Object[] {Oplog.this.toString(), getFileType(olf), getParent().getName()}));
          }
        }
      });
    }
  }

  /**
   * Helper function for the test
   * 
   * @return FileChannel object representing the Oplog
   */
  UninterruptibleFileChannel getFileChannel() {
    return this.crf.channel;
  }

  DirectoryHolder getDirectoryHolder() {
    return this.dirHolder;
  }

  /**
   * The current size of Oplog. It may be less than the actual Oplog file size ( in case of asynch
   * writing as it also takes into account data present in asynch buffers which will get flushed in
   * course of time o
   * 
   * @return long value indicating the current size of the oplog.
   */
  long getOplogSize() {
    return this.crf.currSize + this.drf.currSize;
  }

  boolean isOplogEmpty() {
    return this.crf.currSize <= (OPLOG_DISK_STORE_REC_SIZE + OPLOG_MAGIC_SEQ_REC_SIZE)
        && this.drf.currSize <= (OPLOG_DISK_STORE_REC_SIZE + OPLOG_MAGIC_SEQ_REC_SIZE);
  }

  void incLiveCount() {
    this.totalLiveCount.incrementAndGet();
  }

  private void decLiveCount() {
    this.totalLiveCount.decrementAndGet();
  }

  /**
   * Return true if a record (crf or drf) has been added to this oplog
   */
  boolean hasBeenUsed() {
    return this.hasDeletes.get() || this.totalCount.get() > 0;
  }

  void incTotalCount() {
    if (!isPhase2()) {
      this.totalCount.incrementAndGet();
    }
  }

  private void finishedAppending() {
    // No need to get the backup lock prior to synchronizing (correct lock order) since the
    // synchronized block does not attempt to get the backup lock (incorrect lock order)
    synchronized (this.lock/* crf */) {
      this.doneAppending = true;
    }
    handleNoLiveValues();
    // I'm deadcoding the following because it is not safe unless we change to
    // always recover values. If we don't recover values then
    // an oplog we recovered from may still need to fault values in from memory.
    // if (!getParent().isOverflowEnabled()) {
    // // If !overflow then we can close the file even
    // // when it has recent values because
    // // we will never need to fault values in from this
    // // file since they are all in memory.
    // close();
    // }
  }

  boolean needsCompaction() {
    if (!isCompactionPossible())
      return false;
    if (this.unrecoveredRegionCount.get() > 0)
      return false;
    if (parent.getCompactionThreshold() == 100)
      return true;
    if (parent.getCompactionThreshold() == 0)
      return false;
    // otherwise check if we have enough garbage to collect with a compact
    long rvHWMtmp = this.totalCount.get();
    if (rvHWMtmp > 0) {
      long tlc = this.totalLiveCount.get();
      if (tlc < 0) {
        tlc = 0;
      }
      double rv = tlc;
      if (((rv / (double) rvHWMtmp) * 100) <= parent.getCompactionThreshold()) {
        return true;
      }
    } else {
      return true;
    }
    return false;
  }

  public boolean hadLiveEntries() {
    return this.totalCount.get() != 0;
  }

  public boolean hasNoLiveValues() {
    return this.totalLiveCount.get() <= 0
        // if we have an unrecoveredRegion then we don't know how many liveValues we
        // have
        && this.unrecoveredRegionCount.get() == 0 && !getParent().isOfflineCompacting();
  }

  private void handleEmptyAndOldest(boolean calledByCompactor) {
    if (!calledByCompactor && logger.isDebugEnabled()) {
      logger.debug(
          "Deleting oplog early because it is empty. It is for disk store {} and has oplog#{}",
          getParent().getName(), oplogId);
    }

    destroy();
    getOplogSet().destroyOldestReadyToCompact();
  }

  private void handleEmpty(boolean calledByCompactor) {
    lockCompactor();
    try {
      if (!calledByCompactor) {
        logger.info(LocalizedMessage.create(LocalizedStrings.Oplog_CLOSING_EMPTY_OPLOG_0_1,
            new Object[] {getParent().getName(), toString()}));
      }
      cancelKrf();
      close();
      deleteFiles(getHasDeletes());
    } finally {
      unlockCompactor();
    }
  }

  void cancelKrf() {
    createKrf(true);
  }

  private final static ThreadLocal isCompactorThread = new ThreadLocal();

  private boolean calledByCompactorThread() {
    if (!this.compacting)
      return false;
    Object v = isCompactorThread.get();
    return v != null && v == Boolean.TRUE;
  }

  private void handleNoLiveValues() {
    if (!this.doneAppending)
      return;
    if (hasNoLiveValues()) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        if (calledByCompactorThread()) {
          // after compaction, remove the oplog from the list & destroy it
          CacheObserverHolder.getInstance().beforeDeletingCompactedOplog(this);
        } else {
          CacheObserverHolder.getInstance().beforeDeletingEmptyOplog(this);
        }
      }
      if (isOldest()) {
        if (calledByCompactorThread()) {
          // do it in this compactor thread
          handleEmptyAndOldest(true);
        } else {
          // schedule another thread to do it
          getParent().executeDiskStoreTask(new Runnable() {
            public void run() {
              handleEmptyAndOldest(false);
            }
          });
        }
      } else {
        if (calledByCompactorThread()) {
          // do it in this compactor thread
          handleEmpty(true);
        } else {
          // schedule another thread to do it
          getParent().executeDiskStoreTask(new Runnable() {
            public void run() {
              handleEmpty(false);
            }
          });
        }
      }
    } else if (needsCompaction()) {
      addToBeCompacted();
    }
  }

  private InternalCache getInternalCache() {
    return getParent().getCache();
  }

  /**
   * Return true if this oplog is the oldest one of those ready to compact
   */
  private boolean isOldest() {
    long myId = getOplogId();
    return getOplogSet().isOldestExistingOplog(myId);
  }

  private boolean added = false;

  private synchronized void addToBeCompacted() {
    if (this.added)
      return;
    this.added = true;
    getOplogSet().addToBeCompacted(this);
    if (logger.isDebugEnabled()) {
      logger.debug("Oplog::switchOpLog: Added the Oplog = {} for compacting.", this.oplogId);
    }
  }

  private void initRecoveredEntry(DiskRegionView drv, DiskEntry de) {
    addLive(drv, de);
  }

  /**
   * The oplogId in re points to the oldOplogId. "this" oplog is the current oplog.
   */
  private void updateRecoveredEntry(DiskRegionView drv, DiskEntry de, DiskEntry.RecoveredEntry re) {
    if (getOplogId() != re.getOplogId()) {
      Oplog oldOplog = getOplogSet().getChild(re.getOplogId());
      oldOplog.rmLive(drv, de);
      initRecoveredEntry(drv, de);
    } else {
      getDRI(drv).update(de);
    }
  }

  public void prepareForCompact() {
    this.compacting = true;
  }

  private final Lock compactorLock = new ReentrantLock();

  private void lockCompactor() {
    this.compactorLock.lock();
  }

  private void unlockCompactor() {
    this.compactorLock.unlock();
  }

  /**
   * Copy any live entries last stored in this oplog to the current oplog. No need to copy deletes
   * in the drf. Backup only needs them until all the older crfs are empty.
   */
  public int compact(OplogCompactor compactor) {
    if (!needsCompaction()) {
      return 0; // @todo check new logic that deals with not compacting oplogs
                // which have unrecovered regions
    }
    isCompactorThread.set(Boolean.TRUE);
    assert calledByCompactorThread();
    getParent().acquireCompactorReadLock();
    try {
      if (!compactor.keepCompactorRunning()) {
        return 0;
      }
      lockCompactor();
      try {
        if (hasNoLiveValues()) {
          handleNoLiveValues();
          return 0; // do this while holding compactorLock
        }

        // Start with a fresh wrapper on every compaction so that
        // if previous run used some high memory byte array which was
        // exceptional, it gets garbage collected.
        long opStart = getStats().getStatTime();
        BytesAndBitsForCompactor wrapper = new BytesAndBitsForCompactor();

        DiskEntry de;
        DiskEntry lastDe = null;
        boolean compactFailed = /*
                                 * getParent().getOwner().isDestroyed ||
                                 */!compactor.keepCompactorRunning();
        int totalCount = 0;
        for (DiskRegionInfo dri : this.regionMap.values()) {
          final DiskRegionView dr = dri.getDiskRegion();
          if (dr == null)
            continue;
          boolean didCompact = false;
          while ((de = dri.getNextLiveEntry()) != null) {
            if (/*
                 * getParent().getOwner().isDestroyed ||
                 */!compactor.keepCompactorRunning()) {
              compactFailed = true;
              break;
            }
            if (lastDe != null) {
              if (lastDe == de) {
                throw new IllegalStateException("compactor would have gone into infinite loop");
              }
              assert lastDe != de;
            }
            lastDe = de;
            didCompact = false;
            synchronized (de) { // fix for bug 41797
              DiskId did = de.getDiskId();
              assert did != null;
              synchronized (did) {
                long oplogId = did.getOplogId();
                if (oplogId != getOplogId()) {
                  continue;
                }
                boolean toCompact = getBytesAndBitsForCompaction(dr, de, wrapper);
                if (toCompact) {
                  if (oplogId != did.getOplogId()) {
                    // @todo: Is this even possible? Perhaps I should just assert here
                    // skip this guy his oplogId changed
                    if (!wrapper.isReusable()) {
                      wrapper = new BytesAndBitsForCompactor();
                    } else if (wrapper.getOffHeapData() != null) {
                      wrapper.setOffHeapData(null, (byte) 0);
                    }
                    continue;
                  }
                  // write it to the current oplog
                  getOplogSet().getChild().copyForwardModifyForCompact(dr, de, wrapper);
                  // the did's oplogId will now be set to the current active oplog
                  didCompact = true;
                }
              } // did
            } // de
            if (didCompact) {
              totalCount++;
              getStats().endCompactionUpdate(opStart);
              opStart = getStats().getStatTime();
              // Check if the value byte array happens to be any of the
              // constant
              // static byte arrays or references the value byte array of
              // underlying RegionEntry.
              // If so for preventing data corruption across regions
              // ( in case of static byte arrays) & for RegionEntry,
              // recreate the wrapper
              if (!wrapper.isReusable()) {
                wrapper = new BytesAndBitsForCompactor();
              }
            }
          }
        }

        if (!compactFailed) {
          // Need to still remove the oplog even if it had nothing to compact.
          handleNoLiveValues();

          // We can't assert hasNoLiveValues() because a race condition exists
          // in which our liveEntries list is empty but the liveCount has not
          // yet been decremented.
        }
        return totalCount;
      } finally {
        unlockCompactor();
      }
    } finally {
      getParent().releaseCompactorReadLock();
      assert calledByCompactorThread();
      isCompactorThread.remove();
    }
  }

  public static boolean isCRFFile(String filename) {
    return filename.endsWith(Oplog.CRF_FILE_EXT);
  }

  public static boolean isDRFFile(String filename) {
    return filename.endsWith(Oplog.DRF_FILE_EXT);
  }

  public static String getKRFFilenameFromCRFFilename(String crfFilename) {
    return crfFilename.substring(0, crfFilename.length() - Oplog.CRF_FILE_EXT.length())
        + Oplog.KRF_FILE_EXT;
  }

  long testGetOplogFileLength() throws IOException {
    long result = 0;
    if (this.crf.raf != null) {
      result += this.crf.raf.length();
    }
    if (this.drf.raf != null) {
      result += this.drf.raf.length();
    }
    return result;
  }

  /**
   * This method is called by the async value recovery task to recover the values from the crf if
   * the keys were recovered from the krf.
   */
  public void recoverValuesIfNeeded(Map<Long, DiskRecoveryStore> diskRecoveryStores) {
    // Early out if we start closing the parent.
    if (getParent().isClosing()) {
      return;
    }

    List<KRFEntry> sortedLiveEntries;

    HashMap<Long, DiskRegionInfo> targetRegions = new HashMap<Long, DiskRegionInfo>(this.regionMap);
    synchronized (diskRecoveryStores) {
      Iterator<DiskRecoveryStore> itr = diskRecoveryStores.values().iterator();
      while (itr.hasNext()) {
        DiskRecoveryStore store = itr.next();
        if (isLruValueRecoveryDisabled(store) || store.lruLimitExceeded()) {
          itr.remove();
        }
      }
      // Get the a sorted list of live entries from the target regions
      targetRegions.keySet().retainAll(diskRecoveryStores.keySet());
    }

    sortedLiveEntries = getSortedLiveEntries(targetRegions.values());
    if (sortedLiveEntries == null) {
      // There are no live entries in this oplog to recover.
      return;
    }

    final ByteArrayDataInput in = new ByteArrayDataInput();
    for (KRFEntry entry : sortedLiveEntries) {
      // Early out if we start closing the parent.
      if (getParent().isClosing()) {
        return;
      }

      DiskEntry diskEntry = entry.getDiskEntry();
      DiskRegionView diskRegionView = entry.getDiskRegionView();
      long diskRegionId = diskRegionView.getId();

      // TODO DAN ok, here's what we need to do
      // 1) lock and obtain the correct RegionEntry that we are recovering too.
      // this will likely mean obtaining the correct DiskRecoveryStore, since
      // with
      // that we can find the region entry I believe.
      // 2) Make sure that the lru limit is not exceeded
      // 3) Update the region entry with the value from disk, assuming the value
      // from
      // disk is still valid. That is going to be something like

      synchronized (diskRecoveryStores) {
        DiskRecoveryStore diskRecoveryStore = diskRecoveryStores.get(diskRegionId);
        if (diskRecoveryStore == null) {
          continue;
        }

        // Reset the disk region view because it may have changed
        // due to the region being created.
        diskRegionView = diskRecoveryStore.getDiskRegionView();

        if (diskRegionView == null) {
          continue;
        }
        if (diskRecoveryStore.lruLimitExceeded()) {
          diskRecoveryStores.remove(diskRegionId);
          continue;
        }

        if (diskRegionView.isEntriesMapIncompatible()) {
          // Refetch the disk entry because it may have changed due to copying
          // an incompatible region map
          diskEntry = (DiskEntry) diskRecoveryStore.getRegionMap().getEntryInVM(diskEntry.getKey());
          if (diskEntry == null) {
            continue;
          }
        }

        synchronized (diskEntry) {
          // Make sure the entry hasn't been modified
          if (diskEntry.getDiskId() != null && diskEntry.getDiskId().getOplogId() == oplogId) {
            // dear lord, this goes through a lot of layers. Maybe we should
            // skip some?
            // * specifically, this could end up faulting in from a different
            // oplog, causing
            // us to seek.
            // * Also, there may be lock ordering issues here, Really, I guess I
            // want
            // a flavor of faultInValue that only faults in from this oplog.
            // * We could have some churn here, opening and closing this oplog
            // * We also might not be buffering adjacent entries? Not sure about
            // that one

            // * Ideally, this would fault the thing in only if it were in this
            // oplog and the lru limit wasn't hit
            // and it would return a status if the lru limit was hit to make us
            // remove the store.

            try {
              DiskEntry.Helper.recoverValue(diskEntry, getOplogId(), diskRecoveryStore, in);
            } catch (RegionDestroyedException ignore) {
              // This region has been destroyed, stop recovering from it.
              diskRecoveryStores.remove(diskRegionId);
            }
          }
        }
      }
    }
  }

  private byte[] serializeRVVs(Map<Long, AbstractDiskRegion> drMap, boolean gcRVV)
      throws IOException {
    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);

    // Filter out any regions that do not have versioning enabled
    drMap = new HashMap<Long, AbstractDiskRegion>(drMap);
    for (Iterator<Map.Entry<Long, AbstractDiskRegion>> itr = drMap.entrySet().iterator(); itr
        .hasNext();) {
      Map.Entry<Long, AbstractDiskRegion> regionEntry = itr.next();
      AbstractDiskRegion dr = regionEntry.getValue();
      if (!dr.getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING)) {
        itr.remove();
      }
    }
    // Write the size first
    InternalDataSerializer.writeUnsignedVL(drMap.size(), out);
    // Now write regions RVV.
    for (Map.Entry<Long, AbstractDiskRegion> regionEntry : drMap.entrySet()) {
      // For each region, write the RVV for the region.

      Long diskRegionID = regionEntry.getKey();
      AbstractDiskRegion dr = regionEntry.getValue();

      RegionVersionVector rvv = dr.getRegionVersionVector();
      if (logger.isTraceEnabled(LogMarker.PERSIST_WRITES)) {
        logger.trace(LogMarker.PERSIST_WRITES, "serializeRVVs: isGCRVV={} drId={} rvv={} oplog#{}",
            gcRVV, diskRegionID, rvv.fullToString(), getOplogId());
      }

      // Write the disk region id
      InternalDataSerializer.writeUnsignedVL(diskRegionID, out);

      if (gcRVV) {
        // For the GC RVV, we will just write the GC versions
        Map<VersionSource, Long> memberToVersion = rvv.getMemberToGCVersion();
        InternalDataSerializer.writeUnsignedVL(memberToVersion.size(), out);
        for (Entry<VersionSource, Long> memberEntry : memberToVersion.entrySet()) {

          // For each member, write the canonicalized member id,
          // and the version number for that member
          VersionSource member = memberEntry.getKey();
          Long gcVersion = memberEntry.getValue();

          int id = getParent().getDiskInitFile().getOrCreateCanonicalId(member);
          InternalDataSerializer.writeUnsignedVL(id, out);
          InternalDataSerializer.writeUnsignedVL(gcVersion, out);
        }
      } else {
        DataSerializer.writeBoolean(dr.getRVVTrusted(), out);
        // Otherwise, we will write the version and exception list for each
        // member
        Map<VersionSource, RegionVersionHolder> memberToVersion = rvv.getMemberToVersion();
        InternalDataSerializer.writeUnsignedVL(memberToVersion.size(), out);
        for (Map.Entry<VersionSource, RegionVersionHolder> memberEntry : memberToVersion
            .entrySet()) {

          // For each member, right the canonicalized member id,
          // and the version number with exceptions for that member
          VersionSource member = memberEntry.getKey();
          RegionVersionHolder versionHolder = memberEntry.getValue();
          int id = getParent().getDiskInitFile().getOrCreateCanonicalId(member);
          InternalDataSerializer.writeUnsignedVL(id, out);
          synchronized (versionHolder) {
            InternalDataSerializer.invokeToData(versionHolder, out);
          }
        }
      }
    }
    return out.toByteArray();
  }

  @Override
  public String toString() {
    return "oplog#" + getOplogId();
  }

  /**
   * Method to be used only for testing
   * 
   * @param ch Object to replace the channel in the Oplog.crf
   * @return original channel object
   */
  UninterruptibleFileChannel testSetCrfChannel(UninterruptibleFileChannel ch) {
    UninterruptibleFileChannel chPrev = this.crf.channel;
    this.crf.channel = ch;
    return chPrev;
  }

  private static class OplogFile {
    public File f;
    public UninterruptibleRandomAccessFile raf;
    public volatile boolean RAFClosed = true;
    public UninterruptibleFileChannel channel;
    public ByteBuffer writeBuf;
    public long currSize;
    public long bytesFlushed;
    public boolean unpreblown;
  }

  private static class KRFile {
    public File f;
    FileOutputStream fos;
    BufferedOutputStream bos;
    DataOutputStream dos;
    long lastOffset = 0;
    int keyNum = 0;
  }

  private static String baToString(byte[] ba) {
    return baToString(ba, ba != null ? ba.length : 0);
  }

  private static String baToString(byte[] ba, int len) {
    if (ba == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(ba[i]).append(", ");
    }
    return sb.toString();
  }

  void serializeVersionTag(VersionHolder tag, DataOutput out) throws IOException {
    int entryVersion = tag.getEntryVersion();
    long regionVersion = tag.getRegionVersion();
    VersionSource versionMember = tag.getMemberID();
    long timestamp = tag.getVersionTimeStamp();
    int dsId = tag.getDistributedSystemId();
    serializeVersionTag(entryVersion, regionVersion, versionMember, timestamp, dsId, out);
  }

  byte[] serializeVersionTag(VersionTag tag) throws IOException {
    int entryVersion = tag.getEntryVersion();
    long regionVersion = tag.getRegionVersion();
    VersionSource versionMember = tag.getMemberID();
    long timestamp = tag.getVersionTimeStamp();
    int dsId = tag.getDistributedSystemId();
    return serializeVersionTag(entryVersion, regionVersion, versionMember, timestamp, dsId);
  }

  byte[] serializeVersionTag(VersionStamp stamp) throws IOException {
    int entryVersion = stamp.getEntryVersion();
    long regionVersion = stamp.getRegionVersion();
    VersionSource versionMember = stamp.getMemberID();
    long timestamp = stamp.getVersionTimeStamp();
    int dsId = stamp.getDistributedSystemId();
    return serializeVersionTag(entryVersion, regionVersion, versionMember, timestamp, dsId);
  }

  private byte[] serializeVersionTag(int entryVersion, long regionVersion,
      VersionSource versionMember, long timestamp, int dsId) throws IOException {
    HeapDataOutputStream out = new HeapDataOutputStream(4 + 8 + 4 + 8 + 4, Version.CURRENT);
    serializeVersionTag(entryVersion, regionVersion, versionMember, timestamp, dsId, out);
    return out.toByteArray();
  }

  private void serializeVersionTag(int entryVersion, long regionVersion,
      VersionSource versionMember, long timestamp, int dsId, DataOutput out) throws IOException {
    int memberId = getParent().getDiskInitFile().getOrCreateCanonicalId(versionMember);
    InternalDataSerializer.writeSignedVL(entryVersion, out);
    InternalDataSerializer.writeUnsignedVL(regionVersion, out);
    InternalDataSerializer.writeUnsignedVL(memberId, out);
    InternalDataSerializer.writeUnsignedVL(timestamp, out);
    InternalDataSerializer.writeSignedVL(dsId, out);
  }

  /**
   * Holds all the state for the current operation. Since an oplog can only have one operation in
   * progress at any given time we only need a single instance of this class per oplog.
   */
  private class OpState {
    private byte opCode;
    private byte userBits;
    private boolean notToUseUserBits; // currently only DestroyFromDisk will not
                                      // use userBits
    /**
     * How many bytes it will be when serialized
     */
    private int size;
    private boolean needsValue;
    private ValueWrapper value;
    private int drIdLength; // 1..9
    private final byte[] drIdBytes = new byte[DiskInitFile.DR_ID_MAX_BYTES];
    private byte[] keyBytes;
    private final byte[] deltaIdBytes = new byte[8];
    private int deltaIdBytesLength;
    private long newEntryBase;
    private DiskStoreID diskStoreId;
    private OPLOG_TYPE magic;

    private byte[] versionsBytes;
    private short gfversion;

    public int getSize() {
      return this.size;
    }

    public String debugStr() {
      StringBuilder sb = new StringBuilder();
      sb.append(" opcode=").append(this.opCode).append(" len=").append(this.value.getLength())
          .append(" vb=").append(this.value.getBytesAsString());
      return sb.toString();
    }

    private void write(OplogFile olf, ValueWrapper vw) throws IOException {
      vw.sendTo(olf.writeBuf, Oplog.this);
    }

    private void write(OplogFile olf, byte[] bytes, int byteLength) throws IOException {
      int offset = 0;
      ByteBuffer bb = olf.writeBuf;
      while (offset < byteLength) {

        int bytesThisTime = byteLength - offset;
        boolean needsFlush = false;
        if (bytesThisTime > bb.remaining()) {
          needsFlush = true;
          bytesThisTime = bb.remaining();
        }
        bb.put(bytes, offset, bytesThisTime);
        offset += bytesThisTime;
        if (needsFlush) {
          flushNoSync(olf);
        }
      }
    }

    private void writeByte(OplogFile olf, byte v) throws IOException {
      ByteBuffer bb = olf.writeBuf;
      if (1 > bb.remaining()) {
        flushNoSync(olf);
      }
      bb.put(v);
    }

    private void writeOrdinal(OplogFile olf, short ordinal) throws IOException {
      ByteBuffer bb = olf.writeBuf;
      if (3 > bb.remaining()) {
        flushNoSync(olf);
      }
      // don't compress since we setup fixed size of buffers
      Version.writeOrdinal(bb, ordinal, false);
    }

    private void writeInt(OplogFile olf, int v) throws IOException {
      ByteBuffer bb = olf.writeBuf;
      if (4 > bb.remaining()) {
        flushNoSync(olf);
      }
      bb.putInt(v);
    }

    private void writeLong(OplogFile olf, long v) throws IOException {
      ByteBuffer bb = olf.writeBuf;
      if (8 > bb.remaining()) {
        flushNoSync(olf);
      }
      bb.putLong(v);
    }

    public void initialize(long newEntryBase) {
      this.opCode = OPLOG_NEW_ENTRY_BASE_ID;
      this.newEntryBase = newEntryBase;
      this.size = OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    }

    public void initialize(short gfversion) {
      this.opCode = OPLOG_GEMFIRE_VERSION;
      this.gfversion = gfversion;
      this.size = OPLOG_GEMFIRE_VERSION_REC_SIZE;
    }

    public void initialize(DiskStoreID diskStoreId) {
      this.opCode = OPLOG_DISK_STORE_ID;
      this.diskStoreId = diskStoreId;
      this.size = OPLOG_DISK_STORE_REC_SIZE;
    }

    public void initialize(OPLOG_TYPE magic) {
      this.opCode = OPLOG_MAGIC_SEQ_ID;
      this.magic = magic;
      this.size = OPLOG_MAGIC_SEQ_REC_SIZE;
    }

    public void initialize(Map<Long, AbstractDiskRegion> drMap, boolean gcRVV) throws IOException {
      this.opCode = OPLOG_RVV;
      byte[] rvvBytes = serializeRVVs(drMap, gcRVV);
      this.value = new DiskEntry.Helper.ByteArrayValueWrapper(true, rvvBytes);
      // Size is opCode + length + end of record
      this.size = 1 + rvvBytes.length + 1;
    }

    public void initialize(long oplogKeyId, byte[] keyBytes, byte[] valueBytes, byte userBits,
        long drId, VersionTag tag, boolean notToUseUserBits) throws IOException {
      this.opCode = OPLOG_MOD_ENTRY_WITH_KEY_1ID;
      this.size = 1;// for the opcode
      saveUserBits(notToUseUserBits, userBits);

      this.keyBytes = keyBytes;
      this.value = new DiskEntry.Helper.CompactorValueWrapper(valueBytes, valueBytes.length);
      if (this.userBits == 1 && this.value.getLength() == 0) {
        throw new IllegalStateException("userBits==1 and valueLength is 0");
      }

      this.needsValue = EntryBits.isNeedsValue(this.userBits);
      this.size += (4 + this.keyBytes.length);
      saveDrId(drId);
      initVersionsBytes(tag);

      if (this.needsValue) {
        this.size += 4 + this.value.getLength();
      }
      this.deltaIdBytesLength = 0;
      {
        long delta = calcDelta(oplogKeyId, this.opCode);
        this.deltaIdBytesLength = bytesNeeded(delta);
        this.size += this.deltaIdBytesLength;
        this.opCode += this.deltaIdBytesLength - 1;
        for (int i = this.deltaIdBytesLength - 1; i >= 0; i--) {
          this.deltaIdBytes[i] = (byte) (delta & 0xFF);
          delta >>= 8;
        }
      }

      this.size++; // for END_OF_RECORD_ID
    }

    private void initVersionsBytes(VersionTag tag) throws IOException {
      if (EntryBits.isWithVersions(this.userBits)) {
        this.versionsBytes = serializeVersionTag(tag);
        this.size += this.versionsBytes.length;
      }
    }

    private void initVersionsBytes(DiskEntry entry) throws IOException {
      // persist entry version, region version and memberId
      // The versions in entry are initialized to 0. So we will not persist the
      // 3
      // types of data if region version is 0.

      // TODO: This method will be called 2 times, one for persisting into crf
      // another for persisting into krf, since we did not save the byte arrary
      // for the verstion tag.
      VersionStamp stamp = entry.getVersionStamp();
      if (EntryBits.isWithVersions(this.userBits)) {
        assert (stamp != null);
        this.versionsBytes = serializeVersionTag(stamp);
        this.size += this.versionsBytes.length;
      }
    }

    public void initialize(byte opCode, DiskRegionView dr, DiskEntry entry, ValueWrapper value,
        byte userBits, boolean notToUseUserBits) throws IOException {
      this.opCode = opCode;
      this.size = 1;// for the opcode
      saveUserBits(notToUseUserBits, userBits);

      this.value = value;
      if (this.userBits == 1 && this.value.getLength() == 0) {
        throw new IllegalStateException("userBits==1 and valueLength is 0");
      }

      boolean needsKey = false;
      if (this.opCode == OPLOG_MOD_ENTRY_1ID) {
        if (modNeedsKey(entry)) {
          needsKey = true;
          this.opCode = OPLOG_MOD_ENTRY_WITH_KEY_1ID;
        }
        this.needsValue = EntryBits.isNeedsValue(this.userBits);
        initVersionsBytes(entry);
      } else if (this.opCode == OPLOG_NEW_ENTRY_0ID) {
        needsKey = true;
        this.needsValue = EntryBits.isNeedsValue(this.userBits);
        initVersionsBytes(entry);
      } else if (this.opCode == OPLOG_DEL_ENTRY_1ID) {
        needsKey = false;
        this.needsValue = false;
      }

      if (needsKey) {
        Object key = entry.getKey();
        this.keyBytes = EntryEventImpl.serialize(key);
        this.size += (4 + this.keyBytes.length);
      } else {
        this.keyBytes = null;
      }
      if (this.opCode == OPLOG_DEL_ENTRY_1ID) {
        this.drIdLength = 0;
      } else {
        long drId = dr.getId();
        saveDrId(drId);
      }
      if (this.needsValue) {
        this.size += 4 + this.value.getLength();
      }
      this.deltaIdBytesLength = 0;
      if (this.opCode != OPLOG_NEW_ENTRY_0ID) {
        // if (this.opCode == OPLOG_DEL_ENTRY_1ID) {
        // this.newEntryBase =
        // writeDelEntryId/*abs(entry.getDiskId().getKeyId())*/; this.size += 8;
        // // HACK DEBUG
        // } else {
        // this.newEntryBase =
        // writeModEntryId/*abs(entry.getDiskId().getKeyId())*/; this.size += 8;
        // // HACK DEBUG
        // }
        long keyId = entry.getDiskId().getKeyId();
        if (keyId == 0) {
          Assert.fail("Attempting to write an entry with keyId=0 to oplog. Entry key="
              + entry.getKey() + " diskId=" + entry.getDiskId() + " region=" + dr);
        }
        long delta = calcDelta(keyId, this.opCode);
        this.deltaIdBytesLength = bytesNeeded(delta);
        this.size += this.deltaIdBytesLength;
        this.opCode += this.deltaIdBytesLength - 1;
        for (int i = this.deltaIdBytesLength - 1; i >= 0; i--) {
          this.deltaIdBytes[i] = (byte) (delta & 0xFF);
          delta >>= 8;
        }
      }

      this.size++; // for END_OF_RECORD_ID
    }

    private void saveUserBits(boolean notToUseUserBits, byte userBits) {
      this.notToUseUserBits = notToUseUserBits;
      if (notToUseUserBits) {
        this.userBits = 0;
      } else {
        this.userBits = EntryBits.getPersistentBits(userBits);
        this.size++; // for the userBits
      }
    }

    private void saveDrId(long drId) {
      // If the drId is <= 255 (max unsigned byte) then
      // encode it as a single byte.
      // Otherwise write a byte whose value is the number of bytes
      // it will be encoded by and then follow it with that many bytes.
      // Note that drId are not allowed to have a value in the range 1..8
      // inclusive.
      if (drId >= 0 && drId <= 255) {
        this.drIdLength = 1;
        this.drIdBytes[0] = (byte) drId;
      } else {
        byte bytesNeeded = (byte) Oplog.bytesNeeded(drId);
        this.drIdLength = bytesNeeded + 1;
        this.drIdBytes[0] = bytesNeeded;
        for (int i = bytesNeeded; i >= 1; i--) {
          this.drIdBytes[i] = (byte) (drId & 0xFF);
          drId >>= 8;
        }
      }
      this.size += this.drIdLength;
    }

    public void initialize(byte opCode, long drId, VersionTag tag) throws IOException {
      this.opCode = opCode;
      assert this.opCode == OPLOG_CONFLICT_VERSION;
      this.size = 1;// for the opcode
      saveDrId(drId);

      this.versionsBytes = serializeVersionTag(tag);
      this.size += this.versionsBytes.length;
      this.size++; // for END_OF_RECORD_ID
    }

    /**
     * Returns the offset to the first byte of the value bytes.
     */
    public int getValueOffset() {
      if (!this.needsValue)
        return 0;
      int result = this.deltaIdBytesLength
          // + 8 /* HACK DEBUG */
          + this.drIdLength + 1/* opcode */
          + 4/* value length */;
      if (this.notToUseUserBits == false) {
        result++;
      }
      if (EntryBits.isWithVersions(this.userBits) && this.versionsBytes != null) {
        result += this.versionsBytes.length;
      }

      return result;
    }

    public long write(OplogFile olf) throws IOException {
      long bytesWritten = 0;
      writeByte(olf, this.opCode);
      bytesWritten++;
      if (this.opCode == OPLOG_NEW_ENTRY_BASE_ID) {
        writeLong(olf, this.newEntryBase);
        bytesWritten += 8;
      } else if (this.opCode == OPLOG_DISK_STORE_ID) {
        writeLong(olf, this.diskStoreId.getLeastSignificantBits());
        writeLong(olf, this.diskStoreId.getMostSignificantBits());
        bytesWritten += 16;
      } else if (this.opCode == OPLOG_MAGIC_SEQ_ID) {
        write(olf, this.magic.getBytes(), OPLOG_TYPE.getLen());
        bytesWritten += OPLOG_TYPE.getLen();
      } else if (this.opCode == OPLOG_RVV) {
        write(olf, this.value);
        bytesWritten += this.value.getLength();
      } else if (this.opCode == OPLOG_GEMFIRE_VERSION) {
        writeOrdinal(olf, this.gfversion);
        bytesWritten++;
      } else if (this.opCode == OPLOG_CONFLICT_VERSION) {
        if (this.drIdLength > 0) {
          write(olf, this.drIdBytes, this.drIdLength);
          bytesWritten += this.drIdLength;
        }
        assert this.versionsBytes.length > 0;
        write(olf, this.versionsBytes, this.versionsBytes.length);
        bytesWritten += this.versionsBytes.length;
      } else {
        if (this.notToUseUserBits == false) {
          writeByte(olf, this.userBits);
          bytesWritten++;
        }
        if (this.deltaIdBytesLength > 0) {
          write(olf, this.deltaIdBytes, this.deltaIdBytesLength);
          bytesWritten += this.deltaIdBytesLength;
          // writeLong(olf, this.newEntryBase); bytesWritten += 8; // HACK DEBUG
        }
        if (this.drIdLength > 0) {
          write(olf, this.drIdBytes, this.drIdLength);
          bytesWritten += this.drIdLength;
        }
        if (EntryBits.isWithVersions(this.userBits) && this.versionsBytes != null
            && this.opCode != OPLOG_DEL_ENTRY_1ID) {
          write(olf, this.versionsBytes, this.versionsBytes.length);
          bytesWritten += this.versionsBytes.length;
        }
        if (this.needsValue) {
          int len = this.value.getLength();
          writeInt(olf, len);
          bytesWritten += 4;
          if (len > 0) {
            write(olf, this.value);
            bytesWritten += len;
          }
        }
        if (this.keyBytes != null) {
          writeInt(olf, this.keyBytes.length);
          bytesWritten += 4;
          if (this.keyBytes.length > 0) {
            write(olf, this.keyBytes, this.keyBytes.length);
            bytesWritten += this.keyBytes.length;
          }
        }
      }

      writeByte(olf, END_OF_RECORD_ID);
      bytesWritten++;
      return bytesWritten;
    }

    /**
     * Free up any references to possibly large data.
     */
    public void clear() {
      this.value = null;
      this.keyBytes = null;
      this.notToUseUserBits = false;
      this.versionsBytes = null;
    }
  }

  /**
   * Fake disk entry used to implement the circular linked list of entries an oplog has. Each Oplog
   * will have one OplogDiskEntry whose prev and next fields point to the actual DiskEntrys
   * currently stored in its crf. Items are added at "next" so the most recent entry written will be
   * at next and the oldest item written will be at "prev".
   */
  static class OplogDiskEntry implements DiskEntry, RegionEntry {
    private DiskEntry next = this;
    private DiskEntry prev = this;

    public synchronized DiskEntry getPrev() {
      return this.prev;
    }

    public synchronized void setPrev(DiskEntry v) {
      this.prev = v;
    }

    public synchronized DiskEntry getNext() {
      return this.next;
    }

    public synchronized void setNext(DiskEntry v) {
      this.next = v;
    }

    /**
     * returns the number of entries cleared
     */
    public synchronized int clear(RegionVersionVector rvv,
        Map<DiskEntry, VersionHolder> pendingKrfTags) {
      if (rvv == null) {
        if (pendingKrfTags != null) {
          pendingKrfTags.clear();
        }
        return clear();
      } else {
        // Clearing the list is handled in AbstractRegionMap.clear for RVV
        // based clears, because it removes each entry.
        // It needs to be handled there because the entry is synched at that
        // point
        return 0;
      }
    }

    /**
     * Clear using an RVV. Remove live entries that are contained within the clear RVV.
     */
    private int clearWithRVV(RegionVersionVector rvv, Map<DiskEntry, VersionTag> pendingKrfTags) {
      // TODO this doesn't work, because we can end up removing entries from
      // here before
      // they are removed from the region map. Reverting this to the old, leaky,
      // behavior
      // until I fix the region map code.
      return 0;
    }

    /**
     * Clear without an RVV. Empties the entire list.
     */
    private int clear() {
      int result = 0;
      // Need to iterate over the list and set each prev field to null
      // so that if remove is called it will know that the DiskEntry
      // has already been removed.
      DiskEntry n = getNext();
      setNext(this);
      setPrev(this);
      while (n != this) {
        result++;
        n.setPrev(null);
        n = n.getNext();
      }
      return result;
    }

    public synchronized boolean remove(DiskEntry v) {
      DiskEntry p = v.getPrev();
      if (p != null) {
        v.setPrev(null);
        DiskEntry n = v.getNext();
        v.setNext(null);
        n.setPrev(p);
        p.setNext(n);
        return true;
      } else {
        return false;
      }
    }

    public synchronized void insert(DiskEntry v) {
      assert v.getPrev() == null;
      // checkForDuplicate(v);
      DiskEntry n = getNext();
      setNext(v);
      n.setPrev(v);
      v.setNext(n);
      v.setPrev(this);
    }

    public synchronized void replace(DiskEntry old, DiskEntry v) {
      DiskEntry p = old.getPrev();
      if (p != null) {
        old.setPrev(null);
        v.setPrev(p);
        p.setNext(v);
      }

      DiskEntry n = old.getNext();
      if (n != null) {
        old.setNext(null);
        v.setNext(n);
        n.setPrev(v);
      }

      if (getNext() == old) {
        setNext(v);
      }
    }

    @Override
    public Object getKey() {
      throw new IllegalStateException();
    }

    @Override
    public Object _getValue() {
      throw new IllegalStateException();
    }

    @Override
    public Token getValueAsToken() {
      throw new IllegalStateException();
    }

    @Override
    public void handleValueOverflow(RegionEntryContext context) {
      throw new IllegalStateException();
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext r, Object val, boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    @Override
    public Object _getValueRetain(RegionEntryContext context, boolean decompress) {
      throw new IllegalStateException();
    }

    @Override
    public void setValueWithContext(RegionEntryContext context, Object value) {
      throw new IllegalStateException();
    }

    public void _removePhase1() {
      throw new IllegalStateException();
    }

    public DiskId getDiskId() {
      throw new IllegalStateException();
    }

    public long getLastModified() {
      throw new IllegalStateException();
    }

    public boolean isRecovered() {
      throw new IllegalStateException();
    }

    public boolean isValueNull() {
      throw new IllegalStateException();
    }

    public boolean isRemovedFromDisk() {
      throw new IllegalStateException();
    }

    public int updateAsyncEntrySize(EnableLRU capacityController) {
      throw new IllegalStateException();
    }

    /**
     * Adds any live entries in this list to liveEntries and returns the index of the next free
     * slot.
     * 
     * @param liveEntries the array to fill with the live entries
     * @param idx the first free slot in liveEntries
     * @param drv the disk region these entries are on
     * @return the next free slot in liveEntries
     */
    public synchronized int addLiveEntriesToList(KRFEntry[] liveEntries, int idx,
        DiskRegionView drv, Map<DiskEntry, VersionHolder> pendingKrfTags) {
      DiskEntry de = getPrev();
      while (de != this) {
        VersionHolder tag = null;
        if (pendingKrfTags != null) {
          tag = pendingKrfTags.get(de);
        }
        liveEntries[idx] = new KRFEntry(drv, de, tag);
        idx++;
        de = de.getPrev();
      }
      return idx;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.geode.internal.cache.DiskEntry#getVersionStamp()
     */
    @Override
    public VersionStamp getVersionStamp() {
      // dummy entry as start of live list
      return null;
    }

    @Override
    public boolean hasStats() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long getLastAccessed() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getHitCount() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getMissCount() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
      // TODO Auto-generated method stub
    }

    @Override
    public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
        LocalRegion region, EntryEventImpl event) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setRecentlyUsed() {
      // TODO Auto-generated method stub
    }

    @Override
    public void updateStatsForGet(boolean hit, long time) {
      // TODO Auto-generated method stub
    }

    @Override
    public void txDidDestroy(long currTime) {
      // TODO Auto-generated method stub
    }

    @Override
    public void resetCounts() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
    }

    @Override
    public void makeTombstone(LocalRegion r, VersionTag version) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void removePhase1(LocalRegion r, boolean clear) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void removePhase2() {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isRemovedPhase2() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isTombstone() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean fillInValue(LocalRegion r, InitialImageOperation.Entry entry,
        ByteArrayDataInput in, DM mgr, final Version version) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isOverflowedToDisk(LocalRegion r, DiskPosition dp) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object getValue(RegionEntryContext context) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueRetain(RegionEntryContext context) {
      return null;
    }

    @Override
    public void setValue(RegionEntryContext context, Object value) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void setValueWithTombstoneCheck(Object value, EntryEvent event)
        throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public Object getTransformedValue() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueInVM(RegionEntryContext context) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean initialImagePut(LocalRegion region, long lastModified, Object newValue,
        boolean wasRecovered, boolean acceptedVersionTag) throws RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean initialImageInit(LocalRegion region, long lastModified, Object newValue,
        boolean create, boolean wasRecovered, boolean acceptedVersionTag)
        throws RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean destroy(LocalRegion region, EntryEventImpl event, boolean inTokenMode,
        boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
        boolean removeRecoveredEntry) throws CacheWriterException, EntryNotFoundException,
        TimeoutException, RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean getValueWasResultOfSearch() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setValueResultOfSearch(boolean v) {
      // TODO Auto-generated method stub
    }

    @Override
    public Object getSerializedValueOnDisk(LocalRegion localRegion) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isUpdateInProgress() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setUpdateInProgress(boolean underUpdate) {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isInvalid() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyed() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyedOrRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyedOrRemovedButNotTombstone() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isInvalidOrRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setValueToNull() {
      // TODO Auto-generated method stub
    }

    @Override
    public void returnToPool() {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isCacheListenerInvocationInProgress() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(boolean isListenerInvoked) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
        throws RegionClearedException {}

    @Override
    public boolean isInUseByTransaction() {
      return false;
    }

    @Override
    public void setInUseByTransaction(boolean v) {}

    @Override
    public void incRefCount() {}

    @Override
    public void decRefCount(NewLRUClockHand lruList, LocalRegion lr) {}

    @Override
    public void resetRefCount(NewLRUClockHand lruList) {}

    @Override
    public Object prepareValueForCache(RegionEntryContext r, Object val, EntryEventImpl event,
        boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

  }

  /**
   * Used as the value in the regionMap. Tracks information about what the region has in this oplog.
   */
  public interface DiskRegionInfo {
    public DiskRegionView getDiskRegion();

    public int addLiveEntriesToList(KRFEntry[] liveEntries, int idx);

    public void addLive(DiskEntry de);

    public void update(DiskEntry entry);

    public void replaceLive(DiskEntry old, DiskEntry de);

    public boolean rmLive(DiskEntry de, Oplog oplog);

    public DiskEntry getNextLiveEntry();

    public void setDiskRegion(DiskRegionView dr);

    public long clear(RegionVersionVector rvv);

    /**
     * Return true if we are the first guy to set it to true
     */
    public boolean testAndSetUnrecovered();

    public boolean getUnrecovered();

    /**
     * Return true if we are the first guy to set it to false
     */
    public boolean testAndSetRecovered(DiskRegionView dr);

    /**
     * Callback to indicate that this oplog has created a krf.
     */
    public void afterKrfCreated();
  }

  public abstract static class AbstractDiskRegionInfo implements DiskRegionInfo {
    private DiskRegionView dr;
    private boolean unrecovered = false;

    public AbstractDiskRegionInfo(DiskRegionView dr) {
      this.dr = dr;
    }

    public abstract void addLive(DiskEntry de);

    public abstract DiskEntry getNextLiveEntry();

    public abstract long clear(RegionVersionVector rvv);

    public DiskRegionView getDiskRegion() {
      return this.dr;
    }

    public void setDiskRegion(DiskRegionView dr) {
      this.dr = dr;
    }

    synchronized public boolean testAndSetUnrecovered() {
      boolean result = !this.unrecovered;
      if (result) {
        this.unrecovered = true;
        this.dr = null;
      }
      return result;
    }

    synchronized public boolean getUnrecovered() {
      return this.unrecovered;
    }

    synchronized public boolean testAndSetRecovered(DiskRegionView dr) {
      boolean result = this.unrecovered;
      if (result) {
        this.unrecovered = false;
        this.dr = dr;
      }
      return result;
    }
  }

  public static class DiskRegionInfoNoList extends AbstractDiskRegionInfo {
    private final AtomicInteger liveCount = new AtomicInteger();

    public DiskRegionInfoNoList(DiskRegionView dr) {
      super(dr);
    }

    @Override
    public void addLive(DiskEntry de) {
      this.liveCount.incrementAndGet();
    }

    @Override
    public void update(DiskEntry entry) {
      // nothing to do
    }

    @Override
    public void replaceLive(DiskEntry old, DiskEntry de) {}

    @Override
    public boolean rmLive(DiskEntry de, Oplog oplog) {
      return this.liveCount.decrementAndGet() >= 0;
    }

    @Override
    public DiskEntry getNextLiveEntry() {
      return null;
    }

    @Override
    public long clear(RegionVersionVector rvv) {
      return this.liveCount.getAndSet(0);
    }

    public int addLiveEntriesToList(KRFEntry[] liveEntries, int idx) {
      // nothing needed since no linked list
      return idx;
    }

    public void afterKrfCreated() {
      // do nothing
    }
  }

  public static class DiskRegionInfoWithList extends AbstractDiskRegionInfo {
    /**
     * A linked list of the live entries in this oplog. Updates to pendingKrfTags are protected by
     * synchronizing on object.
     */
    private final OplogDiskEntry liveEntries = new OplogDiskEntry();
    /**
     * A map of DiskEntry to the VersionTag that is written to disk associated with this tag. Only
     * needed for async regions so that we can generate a krf with a version tag that matches the
     * the tag we have written to disk for this oplog.
     */
    private Map<DiskEntry, VersionHolder> pendingKrfTags;

    public DiskRegionInfoWithList(DiskRegionView dr, boolean couldHaveKrf, boolean krfExists) {
      super(dr);
      // we need to keep track of the version tags for entries so that we write
      // the correct entry to the krf
      // both in sync and async disk write cases
      if (!krfExists && couldHaveKrf) {
        pendingKrfTags = new HashMap<DiskEntry, VersionHolder>(200);
      } else {
        pendingKrfTags = null;
      }
    }

    @Override
    public void addLive(DiskEntry de) {
      synchronized (liveEntries) {
        this.liveEntries.insert(de);
        if (pendingKrfTags != null && de.getVersionStamp() != null) {
          // Remember the version tag of the entry as it was written to the crf.
          pendingKrfTags.put(de, new CompactVersionHolder(de.getVersionStamp()));
        }
      }
    }

    @Override
    public void update(DiskEntry de) {
      if (pendingKrfTags != null && de.getVersionStamp() != null) {
        // Remember the version tag of the entry as it was written to the crf.
        pendingKrfTags.put(de, new CompactVersionHolder(de.getVersionStamp()));
      }
    }

    @Override
    public void replaceLive(DiskEntry old, DiskEntry de) {
      synchronized (liveEntries) {
        this.liveEntries.replace(old, de);
        if (pendingKrfTags != null && de.getVersionStamp() != null) {
          // Remember the version tag of the entry as it was written to the crf.
          pendingKrfTags.remove(old);
          pendingKrfTags.put(de, new CompactVersionHolder(de.getVersionStamp()));
        }
      }
    }

    @Override
    public boolean rmLive(DiskEntry de, Oplog oplog) {
      synchronized (liveEntries) {
        boolean removed = this.liveEntries.remove(de);
        if (removed && pendingKrfTags != null) {
          pendingKrfTags.remove(de);
        }
        if (removed) {
          oplog.decLiveCount();
        }
        return removed;
      }
    }

    @Override
    public DiskEntry getNextLiveEntry() {
      DiskEntry result = this.liveEntries.getPrev();
      if (result == this.liveEntries) {
        result = null;
      }
      return result;
    }

    @Override
    public long clear(RegionVersionVector rvv) {
      synchronized (this.liveEntries) {
        return this.liveEntries.clear(rvv, this.pendingKrfTags);
      }
    }

    /**
     * Return true if we are the first guy to set it to true
     */
    @Override
    synchronized public boolean testAndSetUnrecovered() {
      boolean result = super.testAndSetUnrecovered();
      if (result) {
        this.liveEntries.clear();
      }
      return result;
    }

    public int addLiveEntriesToList(KRFEntry[] liveEntries, int idx) {
      synchronized (liveEntries) {
        return this.liveEntries.addLiveEntriesToList(liveEntries, idx, getDiskRegion(),
            pendingKrfTags);
      }
    }

    public void afterKrfCreated() {
      synchronized (liveEntries) {
        this.pendingKrfTags = null;
      }
    }
  }

  /**
   * Used during offline compaction to hold information that may need to be copied forward.
   */
  private static class CompactionRecord {
    private final byte[] keyBytes;
    private long offset;

    public CompactionRecord(byte[] kb, long offset) {
      this.keyBytes = kb;
      this.offset = offset;
    }

    public void update(long offset) {
      this.offset = offset;
    }

    public byte[] getKeyBytes() {
      return this.keyBytes;
    }

    public long getOffset() {
      return this.offset;
    }
  }

  /**
   * Mpa of OplogEntryIds (longs). Memory is optimized by using an int[] for ids in the unsigned int
   * range.
   */
  static class OplogEntryIdMap {

    private final Int2ObjectOpenHashMap ints =
        new Int2ObjectOpenHashMap((int) DiskStoreImpl.INVALID_ID);

    private final Long2ObjectOpenHashMap longs =
        new Long2ObjectOpenHashMap((int) DiskStoreImpl.INVALID_ID);

    public Object put(long id, Object v) {
      Object result;
      if (id == 0) {
        throw new IllegalArgumentException();
      } else if (id > 0 && id <= 0x00000000FFFFFFFFL) {
        result = this.ints.put((int) id, v);
      } else {
        result = this.longs.put(id, v);
      }
      return result;
    }

    public int size() {
      return this.ints.size() + this.longs.size();
    }

    public Object get(long id) {
      Object result;
      if (id >= 0 && id <= 0x00000000FFFFFFFFL) {
        result = this.ints.get((int) id);
      } else {
        result = this.longs.get(id);
      }
      return result;
    }

    public Iterator iterator() {
      return new Iterator();
    }

    public class Iterator {
      private boolean doingInt = true;
      ObjectIterator<Int2ObjectMap.Entry<?>> intIt = ints.int2ObjectEntrySet().fastIterator();
      ObjectIterator<Long2ObjectMap.Entry<?>> longIt = longs.long2ObjectEntrySet().fastIterator();
      Int2ObjectMap.Entry<?> nextIntEntry;
      Long2ObjectMap.Entry<?> nextLongEntry;

      public boolean hasNext() {
        if (this.intIt.hasNext()) {
          return true;
        } else {
          doingInt = false;
          return this.longIt.hasNext();
        }
      }

      public void advance() {
        if (doingInt) {
          nextIntEntry = intIt.next();
        } else {
          nextLongEntry = longIt.next();
        }
      }

      public long key() {
        if (doingInt) {
          return nextIntEntry.getKey();
        } else {
          return nextLongEntry.getKey();
        }
      }

      public Object value() {
        if (doingInt) {
          return nextIntEntry.getValue();
        } else {
          return nextLongEntry.getValue();
        }
      }
    }
  }

  void finishKrf() {
    createKrf(false);
  }

  void prepareForClose() {
    try {
      finishKrf();
    } catch (CancelException e) {
      // workaround for 50465
      if (logger.isDebugEnabled()) {
        logger.debug("Got a cancel exception while creating a krf during shutown", e);
      }
    }
  }

  private Object deserializeKey(byte[] keyBytes, final Version version,
      final ByteArrayDataInput in) {
    if (!getParent().isOffline() || !PdxWriterImpl.isPdx(keyBytes)) {
      return EntryEventImpl.deserialize(keyBytes, version, in);
    } else {
      return new RawByteKey(keyBytes);
    }
  }

  /**
   * If this OpLog is from an older version of the product, then return that {@link Version} else
   * return null.
   */
  public Version getProductVersionIfOld() {
    final Version version = this.gfversion;
    if (version == null) {
      // check for the case of diskstore upgrade from 6.6 to >= 7.0
      if (getParent().isUpgradeVersionOnly()) {
        // assume previous release version
        return Version.GFE_66;
      } else {
        return null;
      }
    } else if (version == Version.CURRENT) {
      return null;
    } else {
      // version changed so return that for VersionedDataStream
      return version;
    }
  }

  /**
   * If this OpLog has data that was written by an older version of the product, then return that
   * {@link Version} else return null.
   */
  public Version getDataVersionIfOld() {
    final Version version = this.dataVersion;
    if (version == null) {
      // check for the case of diskstore upgrade from 6.6 to >= 7.0
      if (getParent().isUpgradeVersionOnly()) {
        // assume previous release version
        return Version.GFE_66;
      } else {
        return null;
      }
    } else if (version == Version.CURRENT) {
      return null;
    } else {
      // version changed so return that for VersionedDataStream
      return version;
    }
  }

  /**
   * Used in offline mode to prevent pdx deserialization of keys. The raw bytes are a serialized
   * pdx.
   * 
   * @since GemFire 6.6
   */
  private static class RawByteKey implements Sendable {
    final byte[] bytes;
    final int hashCode;

    public RawByteKey(byte[] keyBytes) {
      this.bytes = keyBytes;
      this.hashCode = Arrays.hashCode(keyBytes);
    }

    @Override
    public int hashCode() {
      return this.hashCode;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof RawByteKey)) {
        return false;
      }
      return Arrays.equals(this.bytes, ((RawByteKey) other).bytes);
    }

    public void sendTo(DataOutput out) throws IOException {
      out.write(this.bytes);
    }

  }

  /**
   * Enumeration of operation log file types.
   * 
   */
  enum OplogFileType {
    OPLOG_CRF, // Creates and updates
    OPLOG_DRF, // Deletes
    OPLOG_KRF // Keys
  }

  /**
   * Enumeration of the possible results of the okToSkipModifyRecord
   * 
   *
   */
  private static enum OkToSkipResult {
    SKIP_RECORD, // Skip reading the key and value
    SKIP_VALUE, // skip reading just the value
    DONT_SKIP; // don't skip the record

    public boolean skip() {
      return this != DONT_SKIP;
    }

    public boolean skipKey() {
      return this == SKIP_RECORD;
    }

  }

}
