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
package org.apache.geode.internal.cache.tx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;

public class DistTxEntryEvent extends EntryEventImpl {

  protected static final byte HAS_PUTALL_OP = 0x1;
  protected static final byte HAS_REMOVEALL_OP = 0x2;

  private String regionName;

  /**
   * TODO DISTTX: callers of this constructor need to make sure that release is called. In general
   * the distributed tx code needs to be reviewed to see if it correctly handles off-heap.
   */
  @Retained
  public DistTxEntryEvent(EntryEventImpl entry) {
    super(entry);
  }

  // For Serialization
  public DistTxEntryEvent() {}

  public String getRegionName() {
    return regionName;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDSFID() {
    return DIST_TX_OP;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeObject(eventID, out);
    DataSerializer.writeObject(getRegion().getFullPath(), out);
    out.writeByte(op.ordinal);
    DataSerializer.writeObject(getKey(), out);
    DataSerializer.writeInteger(keyInfo.getBucketId(), out);
    DataSerializer.writeObject(basicGetNewValue(), out);

    byte flags = 0;
    if (putAllOp != null) {
      flags |= HAS_PUTALL_OP;
    }
    if (removeAllOp != null) {
      flags |= HAS_REMOVEALL_OP;
    }
    DataSerializer.writeByte(flags, out);

    // handle putAll
    if (putAllOp != null) {
      putAllToData(out, context);
    }
    // handle removeAll
    if (removeAllOp != null) {
      removeAllToData(out, context);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    eventID = DataSerializer.readObject(in);
    regionName = DataSerializer.readString(in);
    op = Operation.fromOrdinal(in.readByte());
    Object key = DataSerializer.readObject(in);
    Integer bucketId = DataSerializer.readInteger(in);
    keyInfo = new DistTxKeyInfo(key, null/*
                                          * value [DISTTX} TODO see if required
                                          */, null/*
                                                  * callbackarg [DISTTX] TODO
                                                  */, bucketId);
    basicSetNewValue(DataSerializer.readObject(in), true);

    byte flags = DataSerializer.readByte(in);

    if ((flags & HAS_PUTALL_OP) != 0) {
      putAllFromData(in, context);
    }

    if ((flags & HAS_REMOVEALL_OP) != 0) {
      removeAllFromData(in, context);
    }
  }

  private void putAllToData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeInteger(putAllOp.putAllDataSize, out);
    EntryVersionsList versionTags = new EntryVersionsList(putAllOp.putAllDataSize);
    boolean hasTags = false;
    final PutAllEntryData[] putAllData = putAllOp.getPutAllEntryData();
    for (int i = 0; i < putAllOp.putAllDataSize; i++) {
      if (!hasTags && putAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = putAllData[i].versionTag;
      versionTags.add(tag);
      putAllData[i].versionTag = null;
      putAllData[i].toData(out, context);
      putAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  private void putAllFromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    int putAllSize = DataSerializer.readInteger(in);
    PutAllEntryData[] putAllEntries = new PutAllEntryData[putAllSize];
    if (putAllSize > 0) {
      final KnownVersion version = StaticSerialization.getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < putAllSize; i++) {
        putAllEntries[i] = new PutAllEntryData(in, context, eventID, i);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < putAllSize; i++) {
          putAllEntries[i].versionTag = versionTags.get(i);
        }
      }
    }
    op = Operation.PUTALL_CREATE;
    setOriginRemote(true);
    setGenerateCallbacks(true);

    putAllOp = new DistributedPutAllOperation(this, putAllSize, false /* [DISTTX] TODO */);
    putAllOp.setPutAllEntryData(putAllEntries);
  }

  private void removeAllToData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeInteger(removeAllOp.removeAllDataSize, out);

    EntryVersionsList versionTags = new EntryVersionsList(removeAllOp.removeAllDataSize);

    boolean hasTags = false;
    final RemoveAllEntryData[] removeAllData = removeAllOp.getRemoveAllEntryData();
    for (int i = 0; i < removeAllOp.removeAllDataSize; i++) {
      if (!hasTags && removeAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = removeAllData[i].versionTag;
      versionTags.add(tag);
      removeAllData[i].versionTag = null;
      removeAllData[i].serializeTo(out, context);
      removeAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  private void removeAllFromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    int removeAllSize = DataSerializer.readInteger(in);
    final RemoveAllEntryData[] removeAllData = new RemoveAllEntryData[removeAllSize];
    final KnownVersion version = StaticSerialization.getVersionForDataStreamOrNull(in);
    final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
    for (int i = 0; i < removeAllSize; i++) {
      removeAllData[i] = new RemoveAllEntryData(in, eventID, i, context);
    }

    boolean hasTags = in.readBoolean();
    if (hasTags) {
      EntryVersionsList versionTags = EntryVersionsList.create(in);
      for (int i = 0; i < removeAllSize; i++) {
        removeAllData[i].versionTag = versionTags.get(i);
      }
    }
    op = Operation.REMOVEALL_DESTROY;
    setOriginRemote(true);
    setGenerateCallbacks(true);

    removeAllOp =
        new DistributedRemoveAllOperation(this, removeAllSize, false /* [DISTTX] TODO */);
    removeAllOp.setRemoveAllEntryData(removeAllData);
  }

  public void setDistributedMember(DistributedMember sender) {
    distributedMember = sender;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(getShortClassName());
    buf.append("[");
    buf.append("eventID=");
    buf.append(eventID);
    if (getRegion() != null) {
      buf.append(";r=").append(getRegion().getName());
    }
    buf.append(";op=");
    buf.append(getOperation());
    buf.append(";key=");
    buf.append(getKey());
    buf.append(";bucket=");
    buf.append(getKeyInfo().getBucketId());
    buf.append(";oldValue=");
    if (putAllOp != null) {
      buf.append(";putAllDataSize :" + putAllOp.putAllDataSize);
    }
    if (removeAllOp != null) {
      buf.append(";removeAllDataSize :" + removeAllOp.removeAllDataSize);
    }
    buf.append("]");
    return buf.toString();
  }
}
