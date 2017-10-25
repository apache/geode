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
package org.apache.geode.internal.cache.entries;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.AbstractDiskRegion;
import org.apache.geode.internal.cache.AbstractLRURegionMap;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskStoreImpl.AsyncDiskEntry;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryBits;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.lru.LRUClockNode;
import org.apache.geode.internal.cache.lru.LRUEntry;
import org.apache.geode.internal.cache.persistence.BytesAndBits;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.AddressableMemoryManager;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Represents an entry in an {@link RegionMap} whose value may be stored on disk. This interface
 * provides accessor and mutator methods for a disk entry's state. This allows us to abstract all of
 * the interesting behavior into a {@linkplain DiskEntry.Helper helper class} that we only need to
 * implement once.
 * <p>
 * Each {@code DiskEntry} has a unique {@code id} that is used by the {@link DiskRegion} to identify
 * the key/value pair. Before the disk entry is written to disk, the value of the {@code id} is
 * {@link DiskRegion#INVALID_ID invalid}. Once the object has been written to disk, the {@code id}
 * is a positive number. If the value is {@linkplain Helper#update updated}, then the {@code id} is
 * negated to signify that the value on disk is dirty.
 *
 * @see DiskRegion
 * @since GemFire 3.2
 */
public interface DiskEntry extends RegionEntry {
  /**
   * Sets the value with a {@link RegionEntryContext}.
   * 
   * @param context the value's context.
   * @param value an entry value.
   */
  public void setValueWithContext(RegionEntryContext context, Object value);

  /**
   * In some cases we need to do something just before we drop the value from a DiskEntry that is
   * being moved (i.e. overflowed) to disk.
   */
  public void handleValueOverflow(RegionEntryContext context);

  /**
   * Returns true if the DiskEntry value is equal to {@link Token#DESTROYED},
   * {@link Token#REMOVED_PHASE1}, or {@link Token#REMOVED_PHASE2}.
   */
  public boolean isRemovedFromDisk();

  /**
   * Returns the id of this {@code DiskEntry}
   */
  public DiskId getDiskId();

  public int updateAsyncEntrySize(EnableLRU capacityController);

  public DiskEntry getPrev();

  public DiskEntry getNext();

  public void setPrev(DiskEntry v);

  public void setNext(DiskEntry v);

  /**
   * Used as the entry value if it was invalidated.
   */
  public static final byte[] INVALID_BYTES = new byte[0];
  /**
   * Used as the entry value if it was locally invalidated.
   */
  public static final byte[] LOCAL_INVALID_BYTES = new byte[0];
  /**
   * Used as the entry value if it was tombstone.
   */
  public static final byte[] TOMBSTONE_BYTES = new byte[0];

  /**
   * A Helper class for performing functions common to all {@code DiskEntry}s.
   */
  public static class Helper {
    private static final Logger logger = LogService.getLogger();

    /**
     * Testing purpose only Get the value of an entry that is on disk without faulting it in and
     * without looking in the io buffer.
     * 
     * @since GemFire 3.2.1
     */
    public static Object getValueOnDisk(DiskEntry entry, DiskRegion dr) {
      DiskId id = entry.getDiskId();
      if (id == null) {
        return null;
      }
      dr.acquireReadLock();
      try {
        synchronized (id) {
          if (id == null || (dr.isBackup() && id.getKeyId() == DiskRegion.INVALID_ID)
              || (!entry.isValueNull() && id.needsToBeWritten()
                  && !EntryBits.isRecoveredFromDisk(id.getUserBits()))/* fix for bug 41942 */) {
            return null;
          }

          return dr.getNoBuffer(id);
        }
      } finally {
        dr.releaseReadLock();
      }
    }

    /**
     * Get the serialized value directly from disk. Returned object may be a
     * {@link CachedDeserializable}. Goes straight to disk without faulting into memory. Only looks
     * at the disk storage, not at heap storage.
     * 
     * @param entry the entry used to identify the value to fetch
     * @param dr the persistent storage from which to fetch the value
     * @return either null, byte array, or CacheDeserializable
     * @since GemFire 57_hotfix
     */
    public static Object getSerializedValueOnDisk(DiskEntry entry, DiskRegion dr) {
      DiskId did = entry.getDiskId();
      if (did == null) {
        return null;
      }
      dr.acquireReadLock();
      try {
        synchronized (did) {
          if (did == null || (dr.isBackup() && did.getKeyId() == DiskRegion.INVALID_ID)) {
            return null;
          } else if (!entry.isValueNull() && did.needsToBeWritten()
              && !EntryBits.isRecoveredFromDisk(did.getUserBits())/* fix for bug 41942 */) {
            return null;
          }
          return dr.getSerializedData(did);
        }
      } finally {
        dr.releaseReadLock();
      }
    }

    /**
     * Get the value of an entry that is on disk without faulting it in . It checks for the presence
     * in the buffer also. This method is used for concurrent map operations and CQ processing
     * 
     * @since GemFire 5.1
     */
    public static Object getValueOnDiskOrBuffer(DiskEntry entry, DiskRegion dr,
        RegionEntryContext context) {
      @Released
      Object v = getOffHeapValueOnDiskOrBuffer(entry, dr, context);
      if (v instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable) v;
        try {
          v = cd.getDeserializedValue(null, null);
        } finally {
          OffHeapHelper.release(cd); // If v was off-heap it is released here
        }
      }
      return v;
    }

    @Retained
    static Object getOffHeapValueOnDiskOrBuffer(DiskEntry entry, DiskRegion dr,
        RegionEntryContext context) {
      DiskId did = entry.getDiskId();
      Object syncObj = did;
      if (syncObj == null) {
        syncObj = entry;
      }
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          if (did != null && did.isPendingAsync()) {
            @Retained
            Object v = entry._getValueRetain(context, true);

            if (Token.isRemovedFromDisk(v)) {
              v = null;
            }
            return v;
          }
          if (did == null || (dr.isBackup() && did.getKeyId() == DiskRegion.INVALID_ID)
              || (!entry.isValueNull() && did.needsToBeWritten()
                  && !EntryBits.isRecoveredFromDisk(did.getUserBits()))/* fix for bug 41942 */) {
            return null;
          }

          return dr.getSerializedData(did);
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
    }

    static boolean isOverflowedToDisk(DiskEntry de, DiskRegion dr,
        DistributedRegion.DiskPosition dp, RegionEntryContext context) {
      Object v = null;
      DiskId did;
      synchronized (de) {
        did = de.getDiskId();
      }
      Object syncObj = did;
      if (syncObj == null) {
        syncObj = de;
      }
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          if (de.isValueNull()) {
            if (did == null) {
              synchronized (de) {
                did = de.getDiskId();
              }
              assert did != null;
              return isOverflowedToDisk(de, dr, dp, context);
            } else {
              dp.setPosition(did.getOplogId(), did.getOffsetInOplog());
              return true;
            }
          } else {
            return false;
          }
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
    }

    /**
     * Get the value of an entry that is on disk without faulting it in.
     * 
     * @since GemFire 3.2.1
     */
    static boolean fillInValue(DiskEntry de, InitialImageOperation.Entry entry, DiskRegion dr,
        DM mgr, ByteArrayDataInput in, RegionEntryContext context, Version version) {
      @Retained
      @Released
      Object v = null;
      DiskId did;
      synchronized (de) {
        did = de.getDiskId();
      }
      Object syncObj = did;
      if (syncObj == null) {
        syncObj = de;
      }
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          entry.setLastModified(mgr, de.getLastModified());

          ReferenceCountHelper.setReferenceCountOwner(entry);

          // OFFHEAP copied to heap entry;
          // TODO: allow entry to refer to offheap since it will be copied to network.
          v = de._getValueRetain(context, true);

          ReferenceCountHelper.setReferenceCountOwner(null);
          if (v == null) {
            if (did == null) {
              // fix for bug 41449
              synchronized (de) {
                did = de.getDiskId();
              }
              assert did != null;
              // do recursive call to get readLock on did
              return fillInValue(de, entry, dr, mgr, in, context, version);
            }
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "DiskEntry.Helper.fillInValue, key={}; getting value from disk, disk id={}",
                  entry.getKey(), did);
            }
            BytesAndBits bb = null;
            try {
              bb = dr.getBytesAndBits(did, false);
            } catch (DiskAccessException ignore) {
              return false;
            }
            if (EntryBits.isInvalid(bb.getBits())) {
              entry.setInvalid();
            } else if (EntryBits.isLocalInvalid(bb.getBits())) {
              entry.setLocalInvalid();
            } else if (EntryBits.isTombstone(bb.getBits())) {
              entry.setTombstone();
            } else {
              entry.setValue(bb.getBytes());
              entry.setSerialized(EntryBits.isSerialized(bb.getBits()));
            }
            return true;
          }
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
      if (Token.isRemovedFromDisk(v)) {
        // fix for bug 31757
        return false;
      } else if (v instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable) v;
        try {
          if (!cd.isSerialized()) {
            entry.setSerialized(false);
            entry.setValue(cd.getDeserializedForReading());

          } else {
            // don't serialize here if it is not already serialized

            Object tmp = cd.getValue();
            if (tmp instanceof byte[]) {
              entry.setValue((byte[]) tmp);
              entry.setSerialized(true);
            } else {
              try {
                HeapDataOutputStream hdos = new HeapDataOutputStream(version);
                BlobHelper.serializeTo(tmp, hdos);
                hdos.trim();
                entry.setValue(hdos);
                entry.setSerialized(true);
              } catch (IOException e) {
                throw new IllegalArgumentException(
                    LocalizedStrings.DiskEntry_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING
                        .toLocalizedString(),
                    e);
              }
            }
          }
        } finally {
          // If v == entry.value then v is assumed to be an OffHeapByteSource
          // and release() will be called on v after the bytes have been read from
          // off-heap.
          if (v != entry.getValue()) {
            OffHeapHelper.releaseWithNoTracking(v);
          }
        }
      } else if (v instanceof byte[]) {
        entry.setValue(v);
        entry.setSerialized(false);
      } else if (v == Token.INVALID) {
        entry.setInvalid();
      } else if (v == Token.LOCAL_INVALID) {
        // fix for bug 31107
        entry.setLocalInvalid();
      } else if (v == Token.TOMBSTONE) {
        entry.setTombstone();
      } else {
        Object preparedValue = v;
        if (preparedValue != null) {
          preparedValue = AbstractRegionEntry.prepareValueForGII(preparedValue);
          if (preparedValue == null) {
            return false;
          }
        }
        {
          try {
            HeapDataOutputStream hdos = new HeapDataOutputStream(version);
            BlobHelper.serializeTo(preparedValue, hdos);
            hdos.trim();
            entry.setValue(hdos);
            entry.setSerialized(true);
          } catch (IOException e) {
            RuntimeException e2 = new IllegalArgumentException(
                LocalizedStrings.DiskEntry_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING
                    .toLocalizedString());
            e2.initCause(e);
            throw e2;
          }
        }
      }
      return true;
    }

    /**
     * Used to initialize a new disk entry
     */
    public static void initialize(DiskEntry entry, DiskRecoveryStore r, Object newValue) {
      DiskRegionView drv = null;
      if (r instanceof LocalRegion) {
        drv = ((LocalRegion) r).getDiskRegion();
      } else if (r instanceof DiskRegionView) {
        drv = (DiskRegionView) r;
      }
      if (drv == null) {
        throw new IllegalArgumentException(
            LocalizedStrings.DiskEntry_DISK_REGION_IS_NULL.toLocalizedString());
      }

      if (newValue instanceof RecoveredEntry) {
        // Set the id directly, the value will also be set if RECOVER_VALUES
        RecoveredEntry re = (RecoveredEntry) newValue;
        DiskId did = entry.getDiskId();
        did.setOplogId(re.getOplogId());
        did.setOffsetInOplog(re.getOffsetInOplog());
        did.setKeyId(re.getRecoveredKeyId());
        did.setUserBits(re.getUserBits());
        did.setValueLength(re.getValueLength());
        if (!re.getValueRecovered()) {
          updateStats(drv, r, 0/* InVM */, 1/* OnDisk */, did.getValueLength());
        } else {
          entry.setValueWithContext(drv,
              entry.prepareValueForCache((RegionEntryContext) r, re.getValue(), false));
          if (!Token.isInvalidOrRemoved(re.getValue())) {
            updateStats(drv, r, 1/* InVM */, 0/* OnDisk */, 0);
          }
        }
      } else {
        DiskId did = entry.getDiskId();
        if (did != null) {
          did.setKeyId(DiskRegion.INVALID_ID);
        }
        if (newValue != null && !Token.isInvalidOrRemoved(newValue)) {
          updateStats(drv, r, 1/* InVM */, 0/* OnDisk */, 0);
        }
      }
    }

    private static final ValueWrapper INVALID_VW = new ByteArrayValueWrapper(true, INVALID_BYTES);
    private static final ValueWrapper LOCAL_INVALID_VW =
        new ByteArrayValueWrapper(true, LOCAL_INVALID_BYTES);
    private static final ValueWrapper TOMBSTONE_VW =
        new ByteArrayValueWrapper(true, TOMBSTONE_BYTES);

    public static interface ValueWrapper {
      public boolean isSerialized();

      public int getLength();

      public byte getUserBits();

      public void sendTo(ByteBuffer bb, Flushable flushable) throws IOException;

      public String getBytesAsString();
    }
    public static interface Flushable {
      public void flush() throws IOException;

      public void flush(ByteBuffer bb, ByteBuffer chunkbb) throws IOException;
    }
    public static class ByteArrayValueWrapper implements ValueWrapper {
      public final boolean isSerializedObject;
      public final byte[] bytes;

      public ByteArrayValueWrapper(boolean isSerializedObject, byte[] bytes) {
        this.isSerializedObject = isSerializedObject;
        this.bytes = bytes;
      }

      @Override
      public boolean isSerialized() {
        return this.isSerializedObject;
      }

      @Override
      public int getLength() {
        return (this.bytes != null) ? this.bytes.length : 0;
      }

      private boolean isInvalidToken() {
        return this == INVALID_VW;
      }

      private boolean isLocalInvalidToken() {
        return this == LOCAL_INVALID_VW;
      }

      private boolean isTombstoneToken() {
        return this == TOMBSTONE_VW;
      }

      @Override
      public byte getUserBits() {
        byte userBits = 0x0;
        if (isSerialized()) {
          if (isTombstoneToken()) {
            userBits = EntryBits.setTombstone(userBits, true);
          } else if (isInvalidToken()) {
            userBits = EntryBits.setInvalid(userBits, true);
          } else if (isLocalInvalidToken()) {
            userBits = EntryBits.setLocalInvalid(userBits, true);
          } else {
            if (this.bytes == null) {
              throw new IllegalStateException("userBits==1 and value is null");
            } else if (this.bytes.length == 0) {
              throw new IllegalStateException("userBits==1 and value is zero length");
            }
            userBits = EntryBits.setSerialized(userBits, true);
          }
        }
        return userBits;
      }

      @Override
      public void sendTo(ByteBuffer bb, Flushable flushable) throws IOException {
        int offset = 0;
        final int maxOffset = getLength();
        while (offset < maxOffset) {
          int bytesThisTime = maxOffset - offset;
          boolean needsFlush = false;
          if (bytesThisTime > bb.remaining()) {
            needsFlush = true;
            bytesThisTime = bb.remaining();
          }
          bb.put(this.bytes, offset, bytesThisTime);
          offset += bytesThisTime;
          if (needsFlush) {
            flushable.flush();
          }
        }
      }

      @Override
      public String getBytesAsString() {
        if (this.bytes == null) {
          return "null";
        }
        StringBuilder sb = new StringBuilder();
        int len = getLength();
        for (int i = 0; i < len; i++) {
          sb.append(this.bytes[i]).append(", ");
        }
        return sb.toString();
      }
    }

    /**
     * This class is a bit of a hack used by the compactor. For the compactor always copies to a
     * byte[] so this class is just a simple wrapper. It is possible that the length of the byte
     * array is greater than the actual length of the wrapped data. At the time we create this we
     * are all done with isSerialized and userBits so those methods are not supported.
     */
    public static class CompactorValueWrapper extends ByteArrayValueWrapper {
      private final int length;

      public CompactorValueWrapper(byte[] bytes, int length) {
        super(false, bytes);
        this.length = length;
      }

      @Override
      public boolean isSerialized() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getLength() {
        return this.length;
      }

      @Override
      public byte getUserBits() {
        throw new UnsupportedOperationException();
      }
    }

    /**
     * Note that the StoredObject this ValueWrapper is created with is unretained so it must be used
     * before the owner of the StoredObject releases it. Since the RegionEntry that has the value we
     * are writing to disk has it retained we are ok as long as this ValueWrapper's life ends before
     * the RegionEntry sync is released. Note that this class is only used with uncompressed
     * StoredObjects.
     */
    public static class OffHeapValueWrapper implements ValueWrapper {
      private final @Unretained StoredObject offHeapData;

      public OffHeapValueWrapper(StoredObject so) {
        assert so.hasRefCount();
        assert !so.isCompressed();
        this.offHeapData = so;
      }

      @Override
      public boolean isSerialized() {
        return this.offHeapData.isSerialized();
      }

      @Override
      public int getLength() {
        return this.offHeapData.getDataSize();
      }

      @Override
      public byte getUserBits() {
        byte userBits = 0x0;
        if (isSerialized()) {
          userBits = EntryBits.setSerialized(userBits, true);
        }
        return userBits;
      }

      @Override
      public void sendTo(ByteBuffer bb, Flushable flushable) throws IOException {
        final int maxOffset = getLength();
        if (maxOffset == 0) {
          return;
        }
        if (maxOffset > bb.capacity()) {
          ByteBuffer chunkbb = this.offHeapData.createDirectByteBuffer();
          if (chunkbb != null) {
            flushable.flush(bb, chunkbb);
            return;
          }
        }
        final long bbAddress = AddressableMemoryManager.getDirectByteBufferAddress(bb);
        if (bbAddress != 0L) {
          int bytesRemaining = maxOffset;
          int availableSpace = bb.remaining();
          long addrToWrite = bbAddress + bb.position();
          long addrToRead = this.offHeapData.getAddressForReadingData(0, maxOffset);
          if (bytesRemaining > availableSpace) {
            do {
              AddressableMemoryManager.copyMemory(addrToRead, addrToWrite, availableSpace);
              bb.position(bb.position() + availableSpace);
              addrToRead += availableSpace;
              bytesRemaining -= availableSpace;
              flushable.flush();
              addrToWrite = bbAddress + bb.position();
              availableSpace = bb.remaining();
            } while (bytesRemaining > availableSpace);
          }
          AddressableMemoryManager.copyMemory(addrToRead, addrToWrite, bytesRemaining);
          bb.position(bb.position() + bytesRemaining);
        } else {
          long addr = this.offHeapData.getAddressForReadingData(0, maxOffset);
          final long endAddr = addr + maxOffset;
          while (addr != endAddr) {
            bb.put(AddressableMemoryManager.readByte(addr));
            addr++;
            if (!bb.hasRemaining()) {
              flushable.flush();
            }
          }
        }
      }

      @Override
      public String getBytesAsString() {
        return this.offHeapData.getStringForm();
      }
    }

    /**
     * Returns true if the given object is off-heap and it is worth wrapping a reference to it
     * instead of copying its data to the heap. Currently all StoredObject's with a refCount are
     * wrapped.
     */
    public static boolean wrapOffHeapReference(Object o) {
      if (o instanceof StoredObject) {
        StoredObject so = (StoredObject) o;
        if (so.hasRefCount()) {
          //
          return true;
        }
      }
      return false;
    }

    public static ValueWrapper createValueWrapper(Object value, EntryEventImpl event) {
      if (value == Token.INVALID) {
        // even though it is not serialized we say it is because
        // bytes will never be an empty array when it is serialized
        // so that gives us a way to specify the invalid value
        // given a byte array and a boolean flag.
        return INVALID_VW;
      } else if (value == Token.LOCAL_INVALID) {
        // even though it is not serialized we say it is because
        // bytes will never be an empty array when it is serialized
        // so that gives us a way to specify the local-invalid value
        // given a byte array and a boolean flag.
        return LOCAL_INVALID_VW;
      } else if (value == Token.TOMBSTONE) {
        return TOMBSTONE_VW;
      } else {
        boolean isSerializedObject = true;
        byte[] bytes;
        if (value instanceof CachedDeserializable) {
          CachedDeserializable proxy = (CachedDeserializable) value;
          if (wrapOffHeapReference(proxy)) {
            return new OffHeapValueWrapper((StoredObject) proxy);
          }
          isSerializedObject = proxy.isSerialized();
          if (isSerializedObject) {
            bytes = proxy.getSerializedValue();
          } else {
            bytes = (byte[]) proxy.getDeserializedForReading();
          }
          if (event != null && isSerializedObject) {
            event.setCachedSerializedNewValue(bytes);
          }
        } else if (value instanceof byte[]) {
          isSerializedObject = false;
          bytes = (byte[]) value;
        } else {
          Assert.assertTrue(!Token.isRemovedFromDisk(value));
          if (event != null && event.getCachedSerializedNewValue() != null) {
            bytes = event.getCachedSerializedNewValue();
          } else {
            bytes = EntryEventImpl.serialize(value);
            if (bytes.length == 0) {
              throw new IllegalStateException(
                  "serializing <" + value + "> produced empty byte array");
            }
            if (event != null) {
              event.setCachedSerializedNewValue(bytes);
            }
          }
        }
        return new ByteArrayValueWrapper(isSerializedObject, bytes);
      }
    }

    public static ValueWrapper createValueWrapperFromEntry(DiskEntry entry, LocalRegion region,
        EntryEventImpl event) {
      if (event != null) {
        // For off-heap it should be faster to pass a reference to the
        // StoredObject instead of using the cached byte[] (unless it is also compressed).
        // Since NIO is used if the chunk of memory is large we can write it
        // to the file using the off-heap memory with no extra copying.
        // So we give preference to getRawNewValue over getCachedSerializedNewValue
        Object rawValue = null;
        {
          rawValue = event.getRawNewValue();
          if (wrapOffHeapReference(rawValue)) {
            return new OffHeapValueWrapper((StoredObject) rawValue);
          }
        }
        if (event.getCachedSerializedNewValue() != null) {
          return new ByteArrayValueWrapper(true, event.getCachedSerializedNewValue());
        }
        if (rawValue != null) {
          return createValueWrapper(rawValue, event);
        }
      }
      @Retained
      Object value = entry._getValueRetain(region, true);
      try {
        return createValueWrapper(value, event);
      } finally {
        OffHeapHelper.release(value);
      }
    }

    private static void writeToDisk(DiskEntry entry, LocalRegion region, boolean async)
        throws RegionClearedException {
      writeToDisk(entry, region, async, null);
    }

    /**
     * Writes the key/value object stored in the given entry to disk
     * 
     * @see DiskRegion#put
     */
    private static void writeToDisk(DiskEntry entry, LocalRegion region, boolean async,
        EntryEventImpl event) throws RegionClearedException {
      writeBytesToDisk(entry, region, async, createValueWrapperFromEntry(entry, region, event));
    }

    private static void writeBytesToDisk(DiskEntry entry, LocalRegion region, boolean async,
        ValueWrapper vw) throws RegionClearedException {
      // @todo does the following unmark need to be called when an async
      // write is scheduled or is it ok for doAsyncFlush to do it?
      entry.getDiskId().unmarkForWriting();
      region.getDiskRegion().put(entry, region, vw, async);
    }

    public static void update(DiskEntry entry, LocalRegion region, Object newValue)
        throws RegionClearedException {
      update(entry, region, newValue, null);
    }

    /**
     * Updates the value of the disk entry with a new value. This allows us to free up disk space in
     * the non-backup case.
     */
    public static void update(DiskEntry entry, LocalRegion region, Object newValue,
        EntryEventImpl event) throws RegionClearedException {
      if (newValue == null) {
        throw new NullPointerException(
            LocalizedStrings.DiskEntry_ENTRYS_VALUE_SHOULD_NOT_BE_NULL.toLocalizedString());
      }

      AsyncDiskEntry asyncDiskEntry = null;
      DiskRegion dr = region.getDiskRegion();
      DiskId did = entry.getDiskId();
      Object syncObj = did;
      if (syncObj == null) {
        syncObj = entry;
      }
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          asyncDiskEntry = basicUpdate(entry, region, newValue, event);
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
      if (asyncDiskEntry != null && did.isPendingAsync()) {
        // this needs to be done outside the above sync
        scheduleAsyncWrite(asyncDiskEntry);
      }
    }

    private static AsyncDiskEntry basicUpdate(DiskEntry entry, LocalRegion region, Object newValue,
        EntryEventImpl event) throws RegionClearedException {
      AsyncDiskEntry result = null;
      DiskRegion dr = region.getDiskRegion();
      DiskId did = entry.getDiskId();
      Token oldValue;
      int oldValueLength;
      oldValue = entry.getValueAsToken();
      if (Token.isRemovedFromDisk(newValue)) {
        if (dr.isBackup()) {
          dr.testIsRecoveredAndClear(did); // fixes bug 41409
        }
        boolean caughtCacheClosed = false;
        try {
          if (!Token.isRemovedFromDisk(oldValue)) {
            result = basicRemoveFromDisk(entry, region, false);
          }
        } catch (CacheClosedException e) {
          caughtCacheClosed = true;
          throw e;
        } finally {
          if (caughtCacheClosed) {
            // 47616: not to set the value to be removedFromDisk since it failed to persist
          } else {
            // Ensure that the value is rightly set despite clear so
            // that it can be distributed correctly
            entry.setValueWithContext(region, newValue); // OFFHEAP newValue was already
                                                         // preparedForCache
          }
        }
      } else if (newValue instanceof RecoveredEntry) {
        ((RecoveredEntry) newValue).applyToDiskEntry(entry, region, dr, did);
      } else {
        // The new value in the entry needs to be set after the disk writing
        // has succeeded.

        // entry.setValueWithContext(region, newValue); // OFFHEAP newValue already prepared

        if (did != null && did.isPendingAsync()) {
          // if the entry was not yet written to disk, we didn't update
          // the bytes on disk.
          oldValueLength = 0;
        } else {
          oldValueLength = getValueLength(did);
        }

        if (dr.isBackup()) {
          dr.testIsRecoveredAndClear(did); // fixes bug 41409
          if (dr.isSync()) {
            if (AbstractRegionEntry.isCompressible(dr, newValue)) {
              // In case of compression the value is being set first
              // so that writeToDisk can get it back from the entry
              // decompressed if it does not have it already in the event.
              // TODO: this may have introduced a bug with clear since
              // writeToDisk can throw RegionClearedException which
              // was supposed to stop us from changing entry.
              entry.setValueWithContext(region, newValue); // OFFHEAP newValue already prepared
              // newValue is prepared and compressed. We can't write compressed values to disk.
              writeToDisk(entry, region, false, event);
            } else {
              writeBytesToDisk(entry, region, false, createValueWrapper(newValue, event));
              entry.setValueWithContext(region, newValue); // OFFHEAP newValue already prepared
            }

          } else {
            // If we have concurrency checks enabled for a persistent region, we need
            // to add an entry to the async queue for every update to maintain the RVV
            boolean maintainRVV = region.isConcurrencyChecksEnabled();

            if (!did.isPendingAsync() || maintainRVV) {
              // if the entry is not async, we need to schedule it
              // for regions with concurrency checks enabled, we add an entry
              // to the queue for every entry.
              did.setPendingAsync(true);
              VersionTag tag = null;
              VersionStamp stamp = entry.getVersionStamp();
              if (stamp != null) {
                tag = stamp.asVersionTag();
              }
              result = new AsyncDiskEntry(region, entry, tag);
            }
            entry.setValueWithContext(region, newValue); // OFFHEAP newValue already prepared
          }
        } else if (did != null) {
          entry.setValueWithContext(region, newValue); // OFFHEAP newValue already prepared

          // Mark the id as needing to be written
          // The disk remove that this section used to do caused bug 30961
          // @todo this seems wrong. How does leaving it on disk fix the bug?
          did.markForWriting();
          // did.setValueSerializedSize(0);
        } else {
          entry.setValueWithContext(region, newValue);
        }

        if (Token.isInvalidOrRemoved(newValue)) {
          if (oldValue == null) {
            updateStats(dr, region, 0/* InVM */, -1/* OnDisk */, -oldValueLength);
          } else if (!Token.isInvalidOrRemoved(oldValue)) {
            updateStats(dr, region, -1/* InVM */, 0/* OnDisk */, 0);
          } else {
            // oldValue was also a token which is neither in vm or on disk.
          }
        } else { // we have a value to put in the vm
          if (oldValue == null) {
            updateStats(dr, region, 1/* InVM */, -1/* OnDisk */, -oldValueLength);
          } else if (Token.isInvalidOrRemoved(oldValue)) {
            updateStats(dr, region, 1/* InVM */, 0/* OnDisk */, 0/* overflowBytesOnDisk */);
          } else {
            // old value was also in vm so no change
          }
        }
      }
      if (entry instanceof LRUEntry) {
        LRUEntry le = (LRUEntry) entry;
        le.unsetEvicted();
      }
      return result;
    }

    private static int getValueLength(DiskId did) {
      int result = 0;
      if (did != null) {
        synchronized (did) {
          result = did.getValueLength();
        }
      }
      return result;
    }

    public static Object getValueInVMOrDiskWithoutFaultIn(DiskEntry entry, LocalRegion region) {
      Object result =
          OffHeapHelper.copyAndReleaseIfNeeded(getValueOffHeapOrDiskWithoutFaultIn(entry, region));
      if (result instanceof CachedDeserializable) {
        result = ((CachedDeserializable) result).getDeserializedValue(null, null);
      }
      return result;
    }

    @Retained
    public static Object getValueOffHeapOrDiskWithoutFaultIn(DiskEntry entry, LocalRegion region) {
      @Retained
      Object v = entry._getValueRetain(region, true);

      if (v == null || Token.isRemovedFromDisk(v) && !region.isIndexCreationThread()) {
        synchronized (entry) {
          v = entry._getValueRetain(region, true);

          if (v == null) {
            v = Helper.getOffHeapValueOnDiskOrBuffer(entry, region.getDiskRegion(), region);
          }
        }
      }
      if (Token.isRemovedFromDisk(v)) {
        // fix for bug 31800
        v = null;
      }
      return v;
    }

    public static Object faultInValue(DiskEntry entry, LocalRegion region) {
      return faultInValue(entry, region, false);
    }

    @Retained
    public static Object faultInValueRetain(DiskEntry entry, LocalRegion region) {
      return faultInValue(entry, region, true);
    }

    /**
     * @param retainResult if true then the result may be a retained off-heap reference
     */
    @Retained
    private static Object faultInValue(DiskEntry entry, LocalRegion region, boolean retainResult) {
      DiskRegion dr = region.getDiskRegion();
      @Retained
      Object v = entry._getValueRetain(region, true);

      boolean lruFaultedIn = false;
      boolean done = false;
      try {
        if (entry instanceof LRUEntry && !dr.isSync()) {
          synchronized (entry) {
            DiskId did = entry.getDiskId();
            if (did != null && did.isPendingAsync()) {
              done = true;
              // See if it is pending async because of a faultOut.
              // If so then if we are not a backup then we can unschedule the pending async.
              // In either case we need to do the lruFaultIn logic.
              boolean evicted = ((LRUClockNode) entry).testEvicted();
              if (evicted) {
                if (!dr.isBackup()) {
                  // @todo do we also need a bit that tells us if it is in the async queue?
                  // Seems like we could end up adding it to the queue multiple times.
                  did.setPendingAsync(false);
                }
              }
              lruEntryFaultIn((LRUEntry) entry, region);
              lruFaultedIn = true;
            }
          }
        }
        if (!done && (v == null || Token.isRemovedFromDisk(v) && !region.isIndexCreationThread())) {
          synchronized (entry) {
            v = entry._getValueRetain(region, true);

            if (v == null) {
              v = readValueFromDisk(entry, region);
              if (entry instanceof LRUEntry) {
                if (v != null && !Token.isInvalid(v)) {
                  lruEntryFaultIn((LRUEntry) entry, region);

                  lruFaultedIn = true;
                }
              }
            }
          }
        }
      } finally {
        if (!retainResult) {
          v = OffHeapHelper.copyAndReleaseIfNeeded(v);
          // At this point v should be either a heap object
        }
      }
      if (Token.isRemoved(v)) {
        // fix for bug 31800
        v = null;
      } else {
        ((RegionEntry) entry).setRecentlyUsed();
      }
      if (lruFaultedIn) {
        lruUpdateCallback(region);
      }
      return v; // OFFHEAP: the value ends up being returned by RegionEntry.getValue
    }

    public static void recoverValue(DiskEntry entry, long oplogId, DiskRecoveryStore recoveryStore,
        ByteArrayDataInput in) {
      boolean lruFaultedIn = false;
      synchronized (entry) {
        if (entry.isValueNull()) {
          DiskId did = entry.getDiskId();
          if (did != null) {
            Object value = null;
            DiskRegionView dr = recoveryStore.getDiskRegionView();
            dr.acquireReadLock();
            try {
              synchronized (did) {
                // don't read if the oplog has changed.
                if (oplogId == did.getOplogId()) {
                  value = getValueFromDisk(dr, did, in);
                  if (value != null) {
                    setValueOnFaultIn(value, did, entry, dr, recoveryStore);
                  }
                }
              }
            } finally {
              dr.releaseReadLock();
            }
            if (entry instanceof LRUEntry) {
              if (value != null && !Token.isInvalid(value)) {
                lruEntryFaultIn((LRUEntry) entry, recoveryStore);
                lruFaultedIn = true;
              }
            }
          }
        }
      }
      if (lruFaultedIn) {
        lruUpdateCallback(recoveryStore);
      }
    }

    /**
     * Caller must have "did" synced.
     */
    private static Object getValueFromDisk(DiskRegionView dr, DiskId did, ByteArrayDataInput in) {
      Object value;
      if (dr.isBackup() && did.getKeyId() == DiskRegion.INVALID_ID) {
        // must have been destroyed
        value = null;
      } else {
        // if a bucket region then create a CachedDeserializable here instead of object
        value = dr.getRaw(did); // fix bug 40192
        if (value instanceof BytesAndBits) {
          BytesAndBits bb = (BytesAndBits) value;
          if (EntryBits.isInvalid(bb.getBits())) {
            value = Token.INVALID;
          } else if (EntryBits.isLocalInvalid(bb.getBits())) {
            value = Token.LOCAL_INVALID;
          } else if (EntryBits.isTombstone(bb.getBits())) {
            value = Token.TOMBSTONE;
          } else if (EntryBits.isSerialized(bb.getBits())) {
            value = readSerializedValue(bb.getBytes(), bb.getVersion(), in, false);
          } else {
            value = readRawValue(bb.getBytes(), bb.getVersion(), in);
          }
        }
      }
      return value;
    }

    private static void lruUpdateCallback(DiskRecoveryStore recoveryStore) {
      /*
       * Used conditional check to see if if its a LIFO Enabled, yes then disable
       * lruUpdateCallback() and called updateStats() its keep track of actual entries present in
       * memory - useful when checking capacity constraint
       */
      try {
        if (recoveryStore.getEvictionAttributes() != null
            && recoveryStore.getEvictionAttributes().getAlgorithm().isLIFO()) {
          ((AbstractLRURegionMap) recoveryStore.getRegionMap()).updateStats();
          return;
        }
        // this must be done after releasing synchronization
        recoveryStore.getRegionMap().lruUpdateCallback();
      } catch (DiskAccessException dae) {
        recoveryStore.handleDiskAccessException(dae);
        throw dae;
      }
    }

    private static void lruEntryFaultIn(LRUEntry entry, DiskRecoveryStore recoveryStore) {
      RegionMap rm = (RegionMap) recoveryStore.getRegionMap();
      try {
        rm.lruEntryFaultIn((LRUEntry) entry);
      } catch (DiskAccessException dae) {
        recoveryStore.handleDiskAccessException(dae);
        throw dae;
      }
    }

    /**
     * Returns the value of this map entry, reading it from disk, if necessary. Sets the value in
     * the entry. This is only called by the faultIn code once it has determined that the value is
     * no longer in memory. Caller must have "entry" synced.
     */
    private static Object readValueFromDisk(DiskEntry entry, DiskRecoveryStore region) {

      DiskRegionView dr = region.getDiskRegionView();
      DiskId did = entry.getDiskId();
      if (did == null) {
        return null;
      }
      dr.acquireReadLock();
      try {
        synchronized (did) {
          Object value = getValueFromDisk(dr, did, null);
          if (value == null)
            return null;
          setValueOnFaultIn(value, did, entry, dr, region);
          return value;
        }
      } finally {
        dr.releaseReadLock();
      }
    }

    /**
     * Caller must have "entry" and "did" synced and "dr" readLocked.
     * 
     * @return the unretained result must be used by the caller before it releases the sync on
     *         "entry".
     */
    @Unretained
    private static Object setValueOnFaultIn(Object value, DiskId did, DiskEntry entry,
        DiskRegionView dr, DiskRecoveryStore region) {
      // dr.getOwner().getCache().getLogger().info("DEBUG: faulting in entry with key " +
      // entry.getKey());
      int bytesOnDisk = getValueLength(did);
      // Retained by the prepareValueForCache call for the region entry.
      // NOTE that we return this value unretained because the retain is owned by the region entry
      // not the caller.
      @Retained
      Object preparedValue = entry.prepareValueForCache((RegionEntryContext) region, value, false);
      region.updateSizeOnFaultIn(entry.getKey(), region.calculateValueSize(preparedValue),
          bytesOnDisk);
      // did.setValueSerializedSize(0);
      // I think the following assertion is true but need to run
      // a regression with it. Reenable this post 6.5
      // Assert.assertTrue(entry._getValue() == null);
      entry.setValueWithContext((RegionEntryContext) region, preparedValue);
      if (!Token.isInvalidOrRemoved(value)) {
        updateStats(dr, region, 1/* InVM */, -1/* OnDisk */, -bytesOnDisk);
      }
      return preparedValue;
    }

    public static Object readSerializedValue(byte[] valueBytes, Version version,
        ByteArrayDataInput in, boolean forceDeserialize) {
      if (forceDeserialize) {
        // deserialize checking for product version change
        return EntryEventImpl.deserialize(valueBytes, version, in);
      } else {
        // TODO: upgrades: is there a case where GemFire values are internal
        // ones that need to be upgraded transparently; probably messages
        // being persisted (gateway events?)
        return CachedDeserializableFactory.create(valueBytes);
      }
    }

    public static Object readRawValue(byte[] valueBytes, Version version, ByteArrayDataInput in) {
      return valueBytes;
    }

    public static void updateStats(DiskRegionView drv, Object owner, int entriesInVmDelta,
        int overflowOnDiskDelta, int overflowBytesOnDiskDelta) {
      if (entriesInVmDelta != 0) {
        drv.incNumEntriesInVM(entriesInVmDelta);
      }
      if (overflowOnDiskDelta != 0) {
        drv.incNumOverflowOnDisk(overflowOnDiskDelta);
      }
      if (overflowBytesOnDiskDelta != 0) {
        drv.incNumOverflowBytesOnDisk(overflowBytesOnDiskDelta);
      }
      if (owner instanceof BucketRegion) {
        BucketRegion br = (BucketRegion) owner;
        br.incNumEntriesInVM(entriesInVmDelta);
        br.incNumOverflowOnDisk(overflowOnDiskDelta);
        br.incNumOverflowBytesOnDisk(overflowBytesOnDiskDelta);
      }
      // Note: we used to call owner.incNumOverflowBytesOnDisk()
      // if owner was a DiskRegionView.
      // But since we also call drv.incNumOverflowBytesOnDisk()
      // and since drv is == owner when owner is not a LocalRegion
      // (see PlaceHolderDiskRegion.getDiskRegionView())
      // this resulted in incNumOverflowBytesOnDisk being called twice.
    }

    /**
     * Writes the value of this {@code DiskEntry} to disk and {@code null} s out the reference to
     * the value to free up VM space.
     * <p>
     * Note that if the value had already been written to disk, it is not written again.
     * <p>
     * Caller must synchronize on entry and it is assumed the entry is evicted
     */
    public static int overflowToDisk(DiskEntry entry, LocalRegion region, EnableLRU ccHelper)
        throws RegionClearedException {
      DiskRegion dr = region.getDiskRegion();
      final int oldSize = region.calculateRegionEntryValueSize(entry);
      // Get diskID . If it is null, it implies it is overflow only mode.
      DiskId did = entry.getDiskId();
      if (did == null) {
        ((LRUEntry) entry).setDelayedDiskId(region);
        did = entry.getDiskId();
      }

      int change = 0;
      boolean scheduledAsyncHere = false;
      dr.acquireReadLock();
      try {
        synchronized (did) {
          // check for a concurrent freeAllEntriesOnDisk which syncs on DiskId but not on the entry
          if (entry.isRemovedFromDisk()) {
            return 0;
          }

          // TODO: Check if we need to overflow even when id is = 0
          boolean wasAlreadyPendingAsync = did.isPendingAsync();
          if (did.needsToBeWritten()) {
            if (dr.isSync()) {
              writeToDisk(entry, region, false);
            } else if (!wasAlreadyPendingAsync) {
              scheduledAsyncHere = true;
              did.setPendingAsync(true);
            } else {
              // it may have been scheduled to be written (isBackup==true)
              // and now we are faulting it out
            }
          }

          // If async then if it does not need to be written (because it already was)
          // then treat it like the sync case. This fixes bug 41310
          if (scheduledAsyncHere || wasAlreadyPendingAsync) {
            // we call _setValue(null) after it is actually written to disk
            change = entry.updateAsyncEntrySize(ccHelper);
            // do the stats when it is actually written to disk
          } else {
            region.updateSizeOnEvict(entry.getKey(), oldSize);
            entry.handleValueOverflow(region);
            entry.setValueWithContext(region, null);
            change = ((LRUClockNode) entry).updateEntrySize(ccHelper);
            // the caller checked to make sure we had something to overflow
            // so dec inVM and inc onDisk
            updateStats(dr, region, -1/* InVM */, 1/* OnDisk */, getValueLength(did));
          }
        }
      } finally {
        dr.releaseReadLock();
      }
      if (scheduledAsyncHere && did.isPendingAsync()) {
        // this needs to be done outside the above sync
        // the version tag is null here because this method only needs
        // to write to disk for overflow only regions, which do not need
        // to maintain an RVV on disk.
        scheduleAsyncWrite(new AsyncDiskEntry(region, entry, null));
      }
      return change;
    }

    private static void scheduleAsyncWrite(AsyncDiskEntry ade) {
      DiskRegion dr = ade.region.getDiskRegion();
      dr.scheduleAsyncWrite(ade);
    }


    public static void handleFullAsyncQueue(DiskEntry entry, LocalRegion region, VersionTag tag) {
      writeEntryToDisk(entry, region, tag, true);
    }

    public static void doAsyncFlush(VersionTag tag, LocalRegion region) {
      if (region.isThisRegionBeingClosedOrDestroyed())
        return;
      DiskRegion dr = region.getDiskRegion();
      if (!dr.isBackup()) {
        return;
      }
      assert !dr.isSync();
      dr.acquireReadLock();
      try {
        dr.getDiskStore().putVersionTagOnly(region, tag, true);
      } finally {
        dr.releaseReadLock();
      }
    }

    /**
     * Flush an entry that was previously scheduled to be written to disk.
     * 
     * @since GemFire prPersistSprint1
     */
    public static void doAsyncFlush(DiskEntry entry, LocalRegion region, VersionTag tag) {
      writeEntryToDisk(entry, region, tag, false);
    }

    /**
     * Does a synchronous write to disk for a region that uses async. This method is used by both
     * doAsyncFlush and handleFullAsyncQueue to fix GEODE-1700.
     * 
     * @param asyncQueueWasFull true if caller wanted to put this entry in the queue but could not
     *        do so because it was full
     */
    private static void writeEntryToDisk(DiskEntry entry, LocalRegion region, VersionTag tag,
        boolean asyncQueueWasFull) {
      if (region.isThisRegionBeingClosedOrDestroyed())
        return;
      DiskRegion dr = region.getDiskRegion();
      if (!asyncQueueWasFull) {
        dr.setClearCountReference();
      }
      synchronized (entry) { // fixes 40116
        // If I don't sync the entry and this method ends up doing an eviction
        // thus setting value to null
        // some other thread is free to fetch the value while the entry is synced
        // and think it has removed it or replaced it. This results in updateSizeOn*
        // being called twice for the same value (once when it is evicted and once
        // when it is removed/updated).
        try {
          dr.acquireReadLock();
          try {
            DiskId did = entry.getDiskId();
            synchronized (did) {
              if (did.isPendingAsync()) {
                did.setPendingAsync(false);
                final Token entryVal = entry.getValueAsToken();
                final int entryValSize = region.calculateRegionEntryValueSize(entry);
                try {
                  if (Token.isRemovedFromDisk(entryVal)) {
                    if (region.isThisRegionBeingClosedOrDestroyed())
                      return;
                    dr.remove(region, entry, true, false);
                    if (dr.isBackup()) {
                      did.setKeyId(DiskRegion.INVALID_ID); // fix for bug 41340
                    }
                  } else if ((Token.isInvalid(entryVal) || entryVal == Token.TOMBSTONE)
                      && !dr.isBackup()) {
                    // no need to write invalid or tombstones to disk if overflow only
                  } else if (entryVal != null) {
                    writeToDisk(entry, region, true);
                    assert !dr.isSync();
                    // Only setValue to null if this was an evict.
                    // We could just be a backup that is writing async.
                    if (!Token.isInvalid(entryVal) && (entryVal != Token.TOMBSTONE)
                        && entry instanceof LRUEntry && ((LRUClockNode) entry).testEvicted()) {
                      // Moved this here to fix bug 40116.
                      region.updateSizeOnEvict(entry.getKey(), entryValSize);
                      updateStats(dr, region, -1/* InVM */, 1/* OnDisk */, did.getValueLength());
                      entry.handleValueOverflow(region);
                      entry.setValueWithContext(region, null);
                    }
                  } else {
                    // if we have a version tag we need to record the operation
                    // to update the RVV
                    if (tag != null) {
                      DiskEntry.Helper.doAsyncFlush(tag, region);
                    }
                    return;
                  }
                } catch (RegionClearedException ignore) {
                  // no need to do the op since it was clobbered by a region clear
                }

                // See if we the entry we wrote to disk has the same tag
                // as this entry. If not, write the tag as a conflicting operation.
                // to update the RVV.
                VersionStamp stamp = entry.getVersionStamp();
                if (tag != null && stamp != null && (stamp.getMemberID() != tag.getMemberID()
                    || stamp.getRegionVersion() != tag.getRegionVersion())) {
                  DiskEntry.Helper.doAsyncFlush(tag, region);
                }
              } else {
                // if we have a version tag we need to record the operation
                // to update the RVV
                if (tag != null) {
                  DiskEntry.Helper.doAsyncFlush(tag, region);
                }
              }
            }
          } finally {
            dr.releaseReadLock();
          }
        } finally {
          if (!asyncQueueWasFull) {
            dr.removeClearCountReference();
          }
        }
      } // sync entry
    }

    /**
     * Removes the key/value pair in the given entry from disk
     *
     * @throws RegionClearedException If the operation is aborted due to a clear
     * @see DiskRegion#remove
     */
    public static void removeFromDisk(DiskEntry entry, LocalRegion region, boolean isClear)
        throws RegionClearedException {
      DiskRegion dr = region.getDiskRegion();
      DiskId did = entry.getDiskId();
      Object syncObj = did;
      if (did == null) {
        syncObj = entry;
      }
      AsyncDiskEntry asyncDiskEntry = null;
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          asyncDiskEntry = basicRemoveFromDisk(entry, region, isClear);
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
      if (asyncDiskEntry != null && did.isPendingAsync()) {
        // do this outside the sync
        scheduleAsyncWrite(asyncDiskEntry);
      }
    }

    private static AsyncDiskEntry basicRemoveFromDisk(DiskEntry entry, LocalRegion region,
        boolean isClear) throws RegionClearedException {
      final DiskRegion dr = region.getDiskRegion();
      final DiskId did = entry.getDiskId();
      final Object curValAsToken = entry.getValueAsToken();
      if (did == null || (dr.isBackup() && did.getKeyId() == DiskRegion.INVALID_ID)) {
        // Not on disk yet
        if (!Token.isInvalidOrRemoved(curValAsToken)) {
          updateStats(dr, region, -1/* InVM */, 0/* OnDisk */, 0);
        }
        dr.unscheduleAsyncWrite(did);
        return null;
      }
      AsyncDiskEntry result = null;

      // Bug # 39989
      did.unmarkForWriting();

      // System.out.println("DEBUG: removeFromDisk doing remove(" + id + ")");
      int oldValueLength = did.getValueLength();
      if (dr.isSync() || isClear) {
        dr.remove(region, entry, false, isClear);
        if (dr.isBackup()) {
          did.setKeyId(DiskRegion.INVALID_ID); // fix for bug 41340
        }
        // If this is a clear, we should unschedule the async write for this
        // entry
        did.setPendingAsync(false);
      } else {
        // If we have concurrency checks enabled for a persistent region, we need
        // to add an entry to the async queue for every update to maintain the RVV
        boolean maintainRVV = region.isConcurrencyChecksEnabled() && dr.isBackup();
        if (!did.isPendingAsync() || maintainRVV) {
          did.setPendingAsync(true);
          VersionTag tag = null;
          VersionStamp stamp = entry.getVersionStamp();
          if (stamp != null) {
            tag = stamp.asVersionTag();
          }
          result = new AsyncDiskEntry(region, entry, tag);
        }
      }
      if (curValAsToken == null) {
        updateStats(dr, region, 0/* InVM */, -1/* OnDisk */, -oldValueLength);
      } else if (!Token.isInvalidOrRemoved(curValAsToken)) {
        updateStats(dr, region, -1/* InVM */, 0/* OnDisk */, 0);
      }
      return result;
    }

    public static void updateVersionOnly(DiskEntry entry, LocalRegion region, VersionTag tag) {
      DiskRegion dr = region.getDiskRegion();
      if (!dr.isBackup()) {
        return;
      }

      assert tag != null && tag.getMemberID() != null;
      boolean scheduleAsync = false;
      DiskId did = entry.getDiskId();
      Object syncObj = did;
      if (syncObj == null) {
        syncObj = entry;
      }
      if (syncObj == did) {
        dr.acquireReadLock();
      }
      try {
        synchronized (syncObj) {
          if (dr.isSync()) {
            dr.getDiskStore().putVersionTagOnly(region, tag, false);
          } else {
            scheduleAsync = true;
          }
        }
      } finally {
        if (syncObj == did) {
          dr.releaseReadLock();
        }
      }
      if (scheduleAsync) {
        // this needs to be done outside the above sync
        scheduleAsyncWrite(new AsyncDiskEntry(region, tag));
      }
    }

  }

  /**
   * A marker object for an entry that has been recovered from disk. It is handled specially when it
   * is placed in a region.
   */
  public static class RecoveredEntry {

    /** The disk id of the entry being recovered */
    private final long recoveredKeyId;

    /** The value of the recovered entry */
    private final Object value;

    private final long offsetInOplog;
    private final byte userBits;
    private final int valueLength;

    private long oplogId;
    private VersionTag tag;

    /** whether the entry value has been faulted in after recovery. */
    private final boolean valueRecovered;

    /**
     * Only for this constructor, the value is not loaded into the region & it is lying on the
     * oplogs. Since Oplogs rely on DiskId to furnish user bits so as to correctly interpret bytes,
     * the userbit needs to be set correctly here.
     */
    public RecoveredEntry(long keyId, long oplogId, long offsetInOplog, byte userBits,
        int valueLength) {
      this(keyId, oplogId, offsetInOplog, userBits, valueLength, null, false);
    }

    public RecoveredEntry(long keyId, long oplogId, long offsetInOplog, byte userBits,
        int valueLength, Object value) {
      this(keyId, oplogId, offsetInOplog, userBits, valueLength, value, true);
    }

    public RecoveredEntry(long keyId, long oplogId, long offsetInOplog, byte userBits,
        int valueLength, Object value, boolean valueRecovered) {
      this.recoveredKeyId = keyId;
      this.value = value;
      this.oplogId = oplogId;
      this.offsetInOplog = offsetInOplog;
      this.userBits = EntryBits.setRecoveredFromDisk(userBits, true);
      this.valueLength = valueLength;
      this.valueRecovered = valueRecovered;
    }

    /**
     * Returns the disk id of the entry being recovered
     */
    public long getRecoveredKeyId() {
      return this.recoveredKeyId;
    }

    /**
     * Returns the value of the recovered entry. Note that if the disk id is < 0 then the value has
     * not been faulted in and this method will return null.
     */
    public Object getValue() {
      return this.value;
    }

    /**
     * @return byte indicating the user bits. The correct value is returned only in the specific
     *         case of entry recovered from oplog ( & not rolled to Htree) & the RECOVER_VALUES flag
     *         is false . In other cases the exact value is not needed
     */
    public byte getUserBits() {
      return this.userBits;
    }

    public int getValueLength() {
      return this.valueLength;
    }

    public long getOffsetInOplog() {
      return offsetInOplog;
    }

    public long getOplogId() {
      return this.oplogId;
    }

    public void setOplogId(long v) {
      this.oplogId = v;
    }

    public boolean getValueRecovered() {
      return this.valueRecovered;
    }

    public VersionTag getVersionTag() {
      return this.tag;
    }

    public void setVersionTag(VersionTag tag) {
      this.tag = tag;
    }

    public void applyToDiskEntry(PlaceHolderDiskRegion drv, DiskEntry entry,
        RegionEntryContext context) {
      DiskId did = entry.getDiskId();
      synchronized (did) {
        applyToDiskEntry(entry, context, drv, did);
      }
    }

    public void applyToDiskEntry(DiskEntry entry, RegionEntryContext region, AbstractDiskRegion dr,
        DiskId did) {
      int oldValueLength;
      // Now that oplog creates are immediately put in cache
      // a later oplog modify will get us here
      Object oldValueAsToken = entry.getValueAsToken();
      boolean oldValueWasNull = oldValueAsToken == null;
      long oldOplogId = did.getOplogId();
      long newOplogId = getOplogId();
      if (newOplogId != oldOplogId) {
        did.setOplogId(newOplogId);
        setOplogId(oldOplogId); // so caller knows oldoplog id
      }
      did.setOffsetInOplog(getOffsetInOplog());
      // id already set
      did.setUserBits(getUserBits());
      oldValueLength = did.getValueLength();
      did.setValueLength(getValueLength());

      if (!getValueRecovered()) {
        if (!oldValueWasNull) {
          entry.handleValueOverflow(region);
          entry.setValueWithContext(region, null); // fixes bug 41119
        }
      } else {
        entry.setValueWithContext(region, entry.prepareValueForCache(region, getValue(), false));
      }

      if (!getValueRecovered()) { // recovering an entry whose new value is on disk
        if (!oldValueWasNull) { // the entry's old value is in vm
          // TODO: oldKeyId == 0 is the ILLEGAL id; what does that indicate?
          int inVM = -1;
          if (Token.isInvalidOrRemoved(oldValueAsToken)) { // but tokens are never in vm
            inVM = 0;
          }
          Helper.updateStats(dr, region, inVM, 1/* OnDisk */, did.getValueLength());
        } else { // the entry's old value is also on disk
          int valueLenDelta = -oldValueLength; // but it is no longer
          valueLenDelta += did.getValueLength(); // new one is now on disk
          Helper.updateStats(dr, region, 0, 0, valueLenDelta);
        }
      } else { // recovering an entry whose new value is in vm
        int inVM = 1;
        if (Token.isInvalidOrRemoved(getValue())) { // but tokens never in vm
          inVM = 0;
        }
        if (oldValueWasNull) { // the entry's old value is on disk
          Helper.updateStats(dr, region, inVM, -1/* OnDisk */, -oldValueLength);
        } else { // the entry's old value was in the vm
          if (inVM == 1 && Token.isInvalidOrRemoved(oldValueAsToken)) {
            // the old state was not in vm and not on disk. But now we are in vm.
            Helper.updateStats(dr, region, 1, 0, 0);
          } else if (inVM == 0 && !Token.isInvalidOrRemoved(oldValueAsToken)) {
            // the old state was in vm and not on disk. But now we are not in vm.
            Helper.updateStats(dr, region, -1, 0, 0);
          }
        }
      }
    }
  }
}
