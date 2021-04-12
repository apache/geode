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
package org.apache.geode.internal.cache.snapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Provides an envelope for transmitting a collection of <code>SnapshotRecord</code>s during export.
 *
 */
public class SnapshotPacket implements DataSerializableFixedID {
  /**
   * Captures the key/value data from a cache entry for import or export.
   */
  public static class SnapshotRecord implements DataSerializableFixedID {
    /** the serialized key */
    private byte[] key;

    /** the serialized value */
    private byte[] value;

    /** for deserialization */
    public SnapshotRecord() {}

    public SnapshotRecord(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    public <K, V> SnapshotRecord(K keyObj, V valObj) throws IOException {
      key = BlobHelper.serializeToBlob(keyObj);
      value = convertToBytes(valObj);
    }

    public <K, V> SnapshotRecord(LocalRegion region, Entry<K, V> entry) throws IOException {
      key = BlobHelper.serializeToBlob(entry.getKey());
      if (entry instanceof NonTXEntry && region != null) {
        @Released
        Object v =
            ((NonTXEntry) entry).getRegionEntry().getValueOffHeapOrDiskWithoutFaultIn(region);
        try {
          value = convertToBytes(v);
        } finally {
          OffHeapHelper.release(v);
        }
      } else if (entry instanceof EntrySnapshot) {
        EntrySnapshot entrySnapshot = (EntrySnapshot) entry;
        Object entryValue = entrySnapshot.getValuePreferringCachedDeserializable();
        value = convertToBytes(entryValue);
      } else {
        value = convertToBytes(entry.getValue());
      }
    }

    /**
     * Returns the serialized key.
     *
     * @return the key
     */
    public byte[] getKey() {
      return key;
    }

    /**
     * Returns the serialized value.
     *
     * @return the value
     */
    public byte[] getValue() {
      return value;
    }

    /**
     * Returns the deserialized key object.
     *
     * @return the key
     *
     * @throws IOException error deserializing key
     * @throws ClassNotFoundException unable to deserialize key
     */
    public <K> K getKeyObject() throws IOException, ClassNotFoundException {
      return (K) BlobHelper.deserializeBlob(key);
    }

    /**
     * Returns the deserialized value object.
     *
     * @return the value
     *
     * @throws IOException error deserializing value
     * @throws ClassNotFoundException unable to deserialize value
     */
    public <V> V getValueObject() throws IOException, ClassNotFoundException {
      return value == null ? null : (V) BlobHelper.deserializeBlob(value);
    }

    /**
     * Returns true if the record has a value.
     *
     * @return the value, or null
     */
    public boolean hasValue() {
      return value != null;
    }

    /**
     * Returns the size in bytes of the serialized key and value.
     *
     * @return the record size
     */
    public int getSize() {
      return key.length + (value == null ? 0 : value.length);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      InternalDataSerializer.writeByteArray(key, out);
      InternalDataSerializer.writeByteArray(value, out);
    }


    @Override
    public int getDSFID() {
      return SNAPSHOT_RECORD;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      key = InternalDataSerializer.readByteArray(in);
      value = InternalDataSerializer.readByteArray(in);
    }

    private byte[] convertToBytes(Object val) throws IOException {
      if (Token.isRemoved(val)) {
        throw new EntryDestroyedException();
      } else if (Token.isInvalid(val)) {
        return null;
      } else if (val instanceof CachedDeserializable) {
        byte[] bytes = ((CachedDeserializable) val).getSerializedValue();
        return bytes;
      } else if (val != null) {
        return BlobHelper.serializeToBlob(val);
      }
      return null;
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }
  }

  /** the window id for responses */
  private int windowId;

  /** the packet id */
  private String packetId;

  /** the member sending the packet */
  private DistributedMember sender;

  /** the snapshot data */
  private SnapshotRecord[] records;

  /** for deserialization */
  public SnapshotPacket() {}

  public SnapshotPacket(int windowId, DistributedMember sender, List<SnapshotRecord> recs) {
    this.windowId = windowId;
    this.packetId = UUID.randomUUID().toString();
    this.sender = sender;
    records = recs.toArray(new SnapshotRecord[0]);
  }

  /**
   * Returns the window id for sending responses.
   *
   * @return the window id
   */
  public int getWindowId() {
    return windowId;
  }

  /**
   * Returns the packet id.
   *
   * @return the packet id
   */
  public String getPacketId() {
    return packetId;
  }

  /**
   * Returns the member that sent the packet.
   *
   * @return the sender
   */
  public DistributedMember getSender() {
    return sender;
  }

  /**
   * Returns the snapshot data
   *
   * @return the records
   */
  public SnapshotRecord[] getRecords() {
    return records;
  }

  @Override
  public int getDSFID() {
    return SNAPSHOT_PACKET;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(windowId);
    InternalDataSerializer.writeString(packetId, out);
    context.getSerializer().writeObject(sender, out);

    InternalDataSerializer.writeArrayLength(records.length, out);
    for (SnapshotRecord rec : records) {
      context.getSerializer().invokeToData(rec, out);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    windowId = in.readInt();
    packetId = InternalDataSerializer.readString(in);
    sender = context.getDeserializer().readObject(in);

    int count = InternalDataSerializer.readArrayLength(in);
    records = new SnapshotRecord[count];

    for (int i = 0; i < count; i++) {
      SnapshotRecord rec = new SnapshotRecord();
      context.getDeserializer().invokeFromData(rec, in);
      records[i] = rec;
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
