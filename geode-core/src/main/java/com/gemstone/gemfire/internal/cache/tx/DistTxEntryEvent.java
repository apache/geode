/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * 
 *
 */
public class DistTxEntryEvent extends EntryEventImpl {
  
  protected static final byte HAS_PUTALL_OP = 0x1;
  protected static final byte HAS_REMOVEALL_OP = 0x2;

  /**
   * TODO DISTTX: callers of this constructor need to
   * make sure that release is called. In general
   * the distributed tx code needs to be reviewed to
   * see if it correctly handles off-heap.
   */
  @Retained
  public DistTxEntryEvent(EntryEventImpl entry) {
    super(entry);
  }
  
  // For Serialization
  public DistTxEntryEvent() {
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDSFID() {
    return DIST_TX_OP;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.eventID, out);
    DataSerializer.writeObject(this.region.getFullPath(), out);
    out.writeByte(this.op.ordinal);
    DataSerializer.writeObject(this.getKey(), out);
    DataSerializer.writeInteger(this.keyInfo.getBucketId(), out);
    DataSerializer.writeObject(this.basicGetNewValue(), out);

    byte flags = 0;
    if (this.putAllOp != null) {
      flags |= HAS_PUTALL_OP;
    }
    if (this.removeAllOp != null) {
      flags |= HAS_REMOVEALL_OP;
    }
    DataSerializer.writeByte(flags, out);
    
    // handle putAll
    if (this.putAllOp != null) {
      putAllToData(out);
    } 
    // handle removeAll
    if (this.removeAllOp != null) {
      removeAllToData(out);
    } 
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.eventID = (EventID) DataSerializer.readObject(in);
    String regionName = DataSerializer.readString(in);
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    this.region = (LocalRegion) cache.getRegion(regionName);
    this.op = Operation.fromOrdinal(in.readByte());
    Object key = DataSerializer.readObject(in);
    Integer bucketId = DataSerializer.readInteger(in);
    this.keyInfo = new DistTxKeyInfo(key, null/*
                                             * value [DISTTX} TODO see if
                                             * required
                                             */, null/*
                                                      * callbackarg [DISTTX]
                                                      * TODO
                                                      */, bucketId);
    basicSetNewValue(DataSerializer.readObject(in));
    
    byte flags = DataSerializer.readByte(in);
    if ((flags & HAS_PUTALL_OP) != 0 ) {
      putAllFromData(in);
    }
    
    if ((flags & HAS_REMOVEALL_OP) != 0 ) {
      removeAllFromData(in);
    }
  }

  /**
   * @param out
   * @throws IOException
   */
  private void putAllToData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.putAllOp.putAllDataSize, out);
    EntryVersionsList versionTags = new EntryVersionsList(
        this.putAllOp.putAllDataSize);
    boolean hasTags = false;
    final PutAllEntryData[] putAllData = this.putAllOp.getPutAllEntryData();
    for (int i = 0; i < this.putAllOp.putAllDataSize; i++) {
      if (!hasTags && putAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = putAllData[i].versionTag;
      versionTags.add(tag);
      putAllData[i].versionTag = null;
      putAllData[i].toData(out);
      putAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  /**
   * @param in
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void putAllFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    int putAllSize = DataSerializer.readInteger(in);
    PutAllEntryData[] putAllEntries = new PutAllEntryData[putAllSize];
    if (putAllSize > 0) {
      final Version version = InternalDataSerializer
          .getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < putAllSize; i++) {
        putAllEntries[i] = new PutAllEntryData(in, this.eventID, i, version,
            bytesIn);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < putAllSize; i++) {
          putAllEntries[i].versionTag = versionTags.get(i);
        }
      }
    }
    // TODO DISTTX: release this event?
    EntryEventImpl e = EntryEventImpl.create(
        this.region, Operation.PUTALL_CREATE,
        null, null, null, true, this.getDistributedMember(), true, true);
    
    this.putAllOp = new DistributedPutAllOperation(e, putAllSize, false /*[DISTTX] TODO*/);
    this.putAllOp.setPutAllEntryData(putAllEntries);
  }
  
  /**
   * @param out
   * @throws IOException
   */
  private void removeAllToData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.removeAllOp.removeAllDataSize, out);
  
    EntryVersionsList versionTags = new EntryVersionsList(
        this.removeAllOp.removeAllDataSize);
  
    boolean hasTags = false;
    final RemoveAllEntryData[] removeAllData = this.removeAllOp
        .getRemoveAllEntryData();
    for (int i = 0; i < this.removeAllOp.removeAllDataSize; i++) {
      if (!hasTags && removeAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = removeAllData[i].versionTag;
      versionTags.add(tag);
      removeAllData[i].versionTag = null;
      removeAllData[i].toData(out);
      removeAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  /**
   * @param in
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void removeAllFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    int removeAllSize = DataSerializer.readInteger(in);
    final RemoveAllEntryData[] removeAllData = new RemoveAllEntryData[removeAllSize];
    final Version version = InternalDataSerializer
        .getVersionForDataStreamOrNull(in);
    final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
    for (int i = 0; i < removeAllSize; i++) {
      removeAllData[i] = new RemoveAllEntryData(in, this.eventID, i, version,
          bytesIn);
    }
  
    boolean hasTags = in.readBoolean();
    if (hasTags) {
      EntryVersionsList versionTags = EntryVersionsList.create(in);
      for (int i = 0; i < removeAllSize; i++) {
        removeAllData[i].versionTag = versionTags.get(i);
      }
    }
    // TODO DISTTX: release this event
    EntryEventImpl e = EntryEventImpl.create(
        this.region, Operation.REMOVEALL_DESTROY,
        null, null, null, true, this.getDistributedMember(), true, true);
    this.removeAllOp = new DistributedRemoveAllOperation(e, removeAllSize, false /*[DISTTX] TODO*/);
    this.removeAllOp.setRemoveAllEntryData(removeAllData);
  }

  public void setDistributedMember(DistributedMember sender) {
    this.distributedMember = sender;
  }
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(getShortClassName());
    buf.append("[");
    buf.append("eventID=");
    buf.append(this.eventID);
    if (this.region != null) {
      buf.append(";r=").append(this.region.getName());
    }
    buf.append(";op=");
    buf.append(getOperation());
    buf.append(";key=");
    buf.append(this.getKey());
    buf.append(";bucket=");
    buf.append(this.getKeyInfo().getBucketId());
    buf.append(";oldValue=");
    if (this.putAllOp != null) {
      buf.append(";putAllDataSize :" + this.putAllOp.putAllDataSize);
    }
    if (this.removeAllOp != null) {
      buf.append(";removeAllDataSize :" + this.removeAllOp.removeAllDataSize);
    }
    buf.append("]");
    return buf.toString();
  }
}
