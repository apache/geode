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
package org.apache.geode.internal.cache.persistence;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CountingDataInputStream;
import org.apache.geode.internal.cache.DiskInitFile;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.Oplog.OPLOG_TYPE;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class DiskInitFileParser {
  private static final Logger logger = LogService.getLogger();

  private final CountingDataInputStream dis;
  private DiskInitFileInterpreter interpreter;

  public DiskInitFileParser(CountingDataInputStream dis, DiskInitFileInterpreter interpreter) {
    this.dis = dis;
    if (logger.isDebugEnabled()) {
      this.interpreter = createPrintingInterpreter(interpreter);
    } else {
      this.interpreter = interpreter;
    }
  }

  private transient boolean gotEOF;

  public DiskStoreID parse() throws IOException, ClassNotFoundException {
    Version gfversion = Version.GFE_662;
    DiskStoreID result = null;
    boolean endOfFile = false;
    while (!endOfFile) {
      if (dis.atEndOfFile()) {
        endOfFile = true;
        break;
      }
      byte opCode = dis.readByte();
      if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "DiskInitFile opcode={}", opCode);
      }
      switch (opCode) {
        case DiskInitFile.IF_EOF_ID:
          endOfFile = true;
          gotEOF = true;
          break;
        case DiskInitFile.IFREC_INSTANTIATOR_ID: {
          int id = dis.readInt();
          String cn = readClassName(dis);
          String icn = readClassName(dis);
          readEndOfRecord(dis);
          interpreter.cmnInstantiatorId(id, cn, icn);
        }
          break;
        case DiskInitFile.IFREC_DATA_SERIALIZER_ID: {
          Class<?> dsc = readClass(dis);
          readEndOfRecord(dis);
          interpreter.cmnDataSerializerId(dsc);
        }
          break;
        case DiskInitFile.IFREC_ONLINE_MEMBER_ID: {
          long drId = readDiskRegionID(dis);
          PersistentMemberID pmid = readPMID(dis, gfversion);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_ONLINE_MEMBER_ID drId={} omid={}", drId, pmid);
          }
          interpreter.cmnOnlineMemberId(drId, pmid);
        }
          break;
        case DiskInitFile.IFREC_OFFLINE_MEMBER_ID: {
          long drId = readDiskRegionID(dis);
          PersistentMemberID pmid = readPMID(dis, gfversion);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_OFFLINE_MEMBER_ID drId={} pmid={}", drId, pmid);
          }
          interpreter.cmnOfflineMemberId(drId, pmid);
        }
          break;
        case DiskInitFile.IFREC_RM_MEMBER_ID: {
          long drId = readDiskRegionID(dis);
          PersistentMemberID pmid = readPMID(dis, gfversion);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_RM_MEMBER_ID drId={} pmid={}",
                drId, pmid);
          }
          interpreter.cmnRmMemberId(drId, pmid);
        }
          break;
        case DiskInitFile.IFREC_MY_MEMBER_INITIALIZING_ID: {
          long drId = readDiskRegionID(dis);
          PersistentMemberID pmid = readPMID(dis, gfversion);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_MY_MEMBER_INITIALIZING_ID drId={} pmid={}", drId, pmid);
          }
          interpreter.cmnAddMyInitializingPMID(drId, pmid);
        }
          break;
        case DiskInitFile.IFREC_MY_MEMBER_INITIALIZED_ID: {
          long drId = readDiskRegionID(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_MY_MEMBER_INITIALIZED_ID drId={}", drId);
          }
          interpreter.cmnMarkInitialized(drId);
        }
          break;
        case DiskInitFile.IFREC_CREATE_REGION_ID: {
          long drId = readDiskRegionID(dis);
          String regName = dis.readUTF();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_CREATE_REGION_ID drId={} name={}",
                drId, regName);
          }
          interpreter.cmnCreateRegion(drId, regName);
        }
          break;
        case DiskInitFile.IFREC_BEGIN_DESTROY_REGION_ID: {
          long drId = readDiskRegionID(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_BEGIN_DESTROY_REGION_ID drId={}", drId);
          }
          interpreter.cmnBeginDestroyRegion(drId);
        }
          break;
        case DiskInitFile.IFREC_END_DESTROY_REGION_ID: {
          long drId = readDiskRegionID(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_END_DESTROY_REGION_ID drId={}",
                drId);
          }
          interpreter.cmnEndDestroyRegion(drId);
        }
          break;
        case DiskInitFile.IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID: {
          long drId = readDiskRegionID(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID drId={}", drId);
          }
          interpreter.cmnBeginPartialDestroyRegion(drId);
        }
          break;
        case DiskInitFile.IFREC_END_PARTIAL_DESTROY_REGION_ID: {
          long drId = readDiskRegionID(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_END_PARTIAL_DESTROY_REGION_ID drId={}", drId);
          }
          interpreter.cmnEndPartialDestroyRegion(drId);
        }
          break;
        case DiskInitFile.IFREC_CLEAR_REGION_ID: {
          long drId = readDiskRegionID(dis);
          long clearOplogEntryId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_CLEAR_REGION_ID drId={} oplogEntryId={}", drId, clearOplogEntryId);
          }
          interpreter.cmnClearRegion(drId, clearOplogEntryId);
        }
          break;
        case DiskInitFile.IFREC_CLEAR_REGION_WITH_RVV_ID: {
          long drId = readDiskRegionID(dis);
          int size = dis.readInt();
          ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion =
              new ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>>(size);
          for (int i = 0; i < size; i++) {
            DiskStoreID id = new DiskStoreID();
            InternalDataSerializer.invokeFromData(id, dis);
            RegionVersionHolder holder = new RegionVersionHolder(dis);
            memberToVersion.put(id, holder);
          }
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_CLEAR_REGION_WITH_RVV_ID drId={} memberToVersion={}", drId, memberToVersion);
          }
          interpreter.cmnClearRegion(drId, memberToVersion);
        }
          break;
        case DiskInitFile.IFREC_CRF_CREATE: {
          long oplogId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_CRF_CREATE oplogId={}",
                oplogId);
          }
          interpreter.cmnCrfCreate(oplogId);
        }
          break;
        case DiskInitFile.IFREC_DRF_CREATE: {
          long oplogId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_DRF_CREATE oplogId={}",
                oplogId);
          }
          interpreter.cmnDrfCreate(oplogId);
        }
          break;
        case DiskInitFile.IFREC_KRF_CREATE: {
          long oplogId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_KRF_CREATE oplogId={}",
                oplogId);
          }
          interpreter.cmnKrfCreate(oplogId);
        }
          break;
        case DiskInitFile.IFREC_CRF_DELETE: {
          long oplogId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_CRF_DELETE oplogId={}",
                oplogId);
          }
          interpreter.cmnCrfDelete(oplogId);
        }
          break;
        case DiskInitFile.IFREC_DRF_DELETE: {
          long oplogId = dis.readLong();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_DRF_DELETE oplogId={}",
                oplogId);
          }
          interpreter.cmnDrfDelete(oplogId);
        }
          break;
        case DiskInitFile.IFREC_REGION_CONFIG_ID: {
          long drId = readDiskRegionID(dis);
          byte lruAlgorithm = dis.readByte();
          byte lruAction = dis.readByte();
          int lruLimit = dis.readInt();
          int concurrencyLevel = dis.readInt();
          int initialCapacity = dis.readInt();
          float loadFactor = dis.readFloat();
          boolean statisticsEnabled = dis.readBoolean();
          boolean isBucket = dis.readBoolean();
          EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_REGION_CONFIG_ID drId={}",
                drId);
          }
          interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit, concurrencyLevel,
              initialCapacity, loadFactor, statisticsEnabled, isBucket, flags,
              ProxyBucketRegion.NO_FIXED_PARTITION_NAME, // fixes bug 43910
              -1, null, false);
        }
          break;
        case DiskInitFile.IFREC_REGION_CONFIG_ID_66: {
          long drId = readDiskRegionID(dis);
          byte lruAlgorithm = dis.readByte();
          byte lruAction = dis.readByte();
          int lruLimit = dis.readInt();
          int concurrencyLevel = dis.readInt();
          int initialCapacity = dis.readInt();
          float loadFactor = dis.readFloat();
          boolean statisticsEnabled = dis.readBoolean();
          boolean isBucket = dis.readBoolean();
          EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
          String partitionName = dis.readUTF();
          int startingBucketId = dis.readInt();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_REGION_CONFIG_ID drId={}",
                drId);
          }
          interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit, concurrencyLevel,
              initialCapacity, loadFactor, statisticsEnabled, isBucket, flags, partitionName,
              startingBucketId, null, false);
        }
          break;
        case DiskInitFile.IFREC_REGION_CONFIG_ID_80: {
          long drId = readDiskRegionID(dis);
          byte lruAlgorithm = dis.readByte();
          byte lruAction = dis.readByte();
          int lruLimit = dis.readInt();
          int concurrencyLevel = dis.readInt();
          int initialCapacity = dis.readInt();
          float loadFactor = dis.readFloat();
          boolean statisticsEnabled = dis.readBoolean();
          boolean isBucket = dis.readBoolean();
          EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
          String partitionName = dis.readUTF();
          int startingBucketId = dis.readInt();

          String compressorClassName = dis.readUTF();
          if ("".equals(compressorClassName)) {
            compressorClassName = null;
          }
          if (dis.readBoolean()) {
            flags.add(DiskRegionFlag.IS_WITH_VERSIONING);
          }
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_REGION_CONFIG_ID drId={}",
                drId);
          }
          interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit, concurrencyLevel,
              initialCapacity, loadFactor, statisticsEnabled, isBucket, flags, partitionName,
              startingBucketId, compressorClassName, false);
        }
          break;
        case DiskInitFile.IFREC_REGION_CONFIG_ID_90: {
          long drId = readDiskRegionID(dis);
          byte lruAlgorithm = dis.readByte();
          byte lruAction = dis.readByte();
          int lruLimit = dis.readInt();
          int concurrencyLevel = dis.readInt();
          int initialCapacity = dis.readInt();
          float loadFactor = dis.readFloat();
          boolean statisticsEnabled = dis.readBoolean();
          boolean isBucket = dis.readBoolean();
          EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
          String partitionName = dis.readUTF();
          int startingBucketId = dis.readInt();

          String compressorClassName = dis.readUTF();
          if ("".equals(compressorClassName)) {
            compressorClassName = null;
          }
          if (dis.readBoolean()) {
            flags.add(DiskRegionFlag.IS_WITH_VERSIONING);
          }
          boolean offHeap = dis.readBoolean();

          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_REGION_CONFIG_ID drId={}",
                drId);
          }
          interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit, concurrencyLevel,
              initialCapacity, loadFactor, statisticsEnabled, isBucket, flags, partitionName,
              startingBucketId, compressorClassName, offHeap);
        }
          break;
        case DiskInitFile.IFREC_OFFLINE_AND_EQUAL_MEMBER_ID: {
          long drId = readDiskRegionID(dis);
          PersistentMemberID pmid = readPMID(dis, gfversion);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_OFFLINE_AND_EQUAL_MEMBER_ID drId={} pmid={}", drId, pmid);
          }
          interpreter.cmdOfflineAndEqualMemberId(drId, pmid);
        }
          break;
        case DiskInitFile.IFREC_DISKSTORE_ID: {
          long leastSigBits = dis.readLong();
          long mostSigBits = dis.readLong();
          readEndOfRecord(dis);
          result = new DiskStoreID(mostSigBits, leastSigBits);
          interpreter.cmnDiskStoreID(result);
        }
          break;
        case DiskInitFile.OPLOG_MAGIC_SEQ_ID:
          readOplogMagicSeqRecord(dis, OPLOG_TYPE.IF);
          break;
        case DiskInitFile.IFREC_PR_CREATE: {
          String name = dis.readUTF();
          int numBuckets = dis.readInt();
          String colocatedWith = dis.readUTF();
          readEndOfRecord(dis);
          PRPersistentConfig config = new PRPersistentConfig(numBuckets, colocatedWith);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_PR_CREATE name={}, config={}",
                name, config);
          }
          interpreter.cmnPRCreate(name, config);
        }
          break;
        case DiskInitFile.IFREC_GEMFIRE_VERSION: {
          short ver = Version.readOrdinal(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_GEMFIRE_VERSION version={}",
                ver);
          }
          try {
            gfversion = Version.fromOrdinal(ver, false);
          } catch (UnsupportedVersionException e) {
            throw new DiskAccessException(
                String.format("Unknown version ordinal %s found when recovering Oplogs", ver), e,
                this.interpreter.getNameForError());
          }
          interpreter.cmnGemfireVersion(gfversion);
          break;
        }
        case DiskInitFile.IFREC_PR_DESTROY: {
          String name = dis.readUTF();
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "IFREC_PR_DESTROY name={}", name);
          }
          interpreter.cmnPRDestroy(name);
        }
          break;
        case DiskInitFile.IFREC_ADD_CANONICAL_MEMBER_ID: {
          int id = dis.readInt();
          Object object = DataSerializer.readObject(dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_ADD_CANONICAL_MEMBER_ID id={} name={}", id, object);
          }
          interpreter.cmnAddCanonicalMemberId(id, object);
          break;
        }
        case DiskInitFile.IFREC_REVOKE_DISK_STORE_ID: {
          PersistentMemberPattern pattern = new PersistentMemberPattern();
          InternalDataSerializer.invokeFromData(pattern, dis);
          readEndOfRecord(dis);
          if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
            logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
                "IFREC_REVOKE_DISK_STORE_ID id={}" + pattern);
          }
          interpreter.cmnRevokeDiskStoreId(pattern);
        }
          break;
        default:
          throw new DiskAccessException(
              String.format("Unknown opCode %s found in disk initialization file.", opCode),
              this.interpreter.getNameForError());
      }
      if (interpreter.isClosing()) {
        break;
      }
    }
    return result;
  }

  private void readOplogMagicSeqRecord(DataInput dis, OPLOG_TYPE type) throws IOException {
    byte[] seq = new byte[OPLOG_TYPE.getLen()];
    dis.readFully(seq);
    for (int i = 0; i < OPLOG_TYPE.getLen(); i++) {
      if (seq[i] != type.getBytes()[i]) {
        if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
          logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE,
              "oplog magic code mismatched at byte:{}, value:{}", (i + 1), seq[i]);
        }
        throw new DiskAccessException("Invalid oplog (" + type.name() + ") file provided.",
            interpreter.getNameForError());
      }
    }
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < OPLOG_TYPE.getLen(); i++) {
        sb.append(" " + seq[i]);
      }
      logger.trace(LogMarker.PERSIST_RECOVERY_VERBOSE, "oplog magic code: {}", sb);
    }
    readEndOfRecord(dis);
  }

  /**
   * Reads a class name from the given input stream, as written by writeClass, and loads the class.
   *
   * @return null if class can not be loaded; otherwise loaded Class
   */
  private static Class<?> readClass(DataInput di) throws IOException {
    int len = di.readInt();
    byte[] bytes = new byte[len];
    di.readFully(bytes);
    String className = new String(bytes); // use default decoder
    Class<?> result = null;
    try {
      result = InternalDataSerializer.getCachedClass(className); // see bug 41206
    } catch (ClassNotFoundException ignore) {
    }
    return result;
  }

  /**
   * Reads a class name from the given input stream.
   *
   * @return class name
   */
  private static String readClassName(DataInput di) throws IOException {
    int len = di.readInt();
    byte[] bytes = new byte[len];
    di.readFully(bytes);
    return new String(bytes); // use default decoder
  }

  static long readDiskRegionID(CountingDataInputStream dis) throws IOException {
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

  private void readEndOfRecord(DataInput di) throws IOException {
    int b = di.readByte();
    if (b != DiskInitFile.END_OF_RECORD_ID) {
      if (b == 0) {
        // this is expected if this is the last record and we died while writing it.
        throw new EOFException("found partial last record");
      } else {
        // Our implementation currently relies on all unwritten bytes having
        // a value of 0. So throw this exception if we find one we didn't expect.
        throw new IllegalStateException("expected end of record (byte=="
            + DiskInitFile.END_OF_RECORD_ID + ") or zero but found " + b);
      }
    }
  }

  private PersistentMemberID readPMID(CountingDataInputStream dis, Version gfversion)
      throws IOException, ClassNotFoundException {
    int len = dis.readInt();
    byte[] buf = new byte[len];
    dis.readFully(buf);
    return bytesToPMID(buf, gfversion);
  }

  private PersistentMemberID bytesToPMID(byte[] bytes, Version gfversion)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    PersistentMemberID result = new PersistentMemberID();
    if (Version.GFE_70.compareTo(gfversion) > 0) {
      result._fromData662(dis);
    } else {
      InternalDataSerializer.invokeFromData(result, dis);
    }
    return result;
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    if (args.length != 1) {
      System.err.println("Usage: parse filename");
      ExitCode.FATAL.doSystemExit();
    }
    dump(new File(args[0]));
  }

  public static void dump(File file) throws IOException, ClassNotFoundException {
    InputStream is = new FileInputStream(file);
    CountingDataInputStream dis = new CountingDataInputStream(is, file.length());

    try {
      DiskInitFileInterpreter interpreter = createPrintingInterpreter(null);
      DiskInitFileParser parser = new DiskInitFileParser(dis, interpreter);
      parser.parse();
    } finally {
      is.close();
    }
  }

  private static DiskInitFileInterpreter createPrintingInterpreter(
      DiskInitFileInterpreter wrapped) {
    DiskInitFileInterpreter interpreter = (DiskInitFileInterpreter) Proxy.newProxyInstance(
        DiskInitFileInterpreter.class.getClassLoader(), new Class[] {DiskInitFileInterpreter.class},
        new PrintingInterpreter(wrapped));
    return interpreter;
  }


  private static class PrintingInterpreter implements InvocationHandler {

    private final DiskInitFileInterpreter delegate;

    public PrintingInterpreter(DiskInitFileInterpreter wrapped) {
      this.delegate = wrapped;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getName().equals("isClosing")) {
        if (delegate == null) {
          return Boolean.FALSE;
        } else {
          return delegate.isClosing();
        }
      }
      Object result = null;
      if (method.getReturnType().equals(boolean.class)) {
        result = Boolean.TRUE;
      }

      StringBuilder out = new StringBuilder();
      out.append(method.getName()).append("(");
      for (Object arg : args) {
        out.append(arg);
        out.append(",");
      }
      out.replace(out.length() - 1, out.length(), ")");
      if (logger.isDebugEnabled()) {
        logger.debug(out.toString());
      }
      if (delegate == null) {
        return result;
      } else {
        return method.invoke(delegate, args);
      }
    }

  }

  public boolean gotEOF() {
    return this.gotEOF;
  }
}
