package com.gemstone.gemfire.internal.cache.tx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.EntryEvent;
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
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author shirishd
 *
 */
public class DistTxEntryEvent extends EntryEventImpl {

  // For Serialization
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

    // handle putAll
    if (this.putAllOp != null) {
      putAllToData(out);
    } else {
      DataSerializer.writeInteger(0, out);
    }

    // handle removeAll
    if (this.removeAllOp != null) {
      removeAllToData(out);
    } else {
      DataSerializer.writeInteger(0, out);
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
    int putAllSize = DataSerializer.readInteger(in);
    if (putAllSize > 0) {
      putAllFromData(in, putAllSize);
    }
    int removeAllSize = DataSerializer.readInteger(in);
    if (removeAllSize > 0) {
      removeAllFromData(in, removeAllSize);
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
    // get the "keyRequiresRegionContext" flag from first element assuming
    // all key objects to be uniform
    final PutAllEntryData[] putAllData = this.putAllOp.getPutAllEntryData();
    // final boolean requiresRegionContext =
    // (putAllData[0].key instanceof KeyWithRegionContext);
    final boolean requiresRegionContext = false;
    for (int i = 0; i < this.putAllOp.putAllDataSize; i++) {
      if (!hasTags && putAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = putAllData[i].versionTag;
      versionTags.add(tag);
      putAllData[i].versionTag = null;
      putAllData[i].toData(out, requiresRegionContext);
      putAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  /**
   * @param in
   * @param putAllSize
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void putAllFromData(DataInput in, int putAllSize)
      throws IOException, ClassNotFoundException {
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
    // get the "keyRequiresRegionContext" flag from first element assuming
    // all key objects to be uniform
    // final boolean requiresRegionContext =
    // (this.removeAllData[0].key instanceof KeyWithRegionContext);
    final RemoveAllEntryData[] removeAllData = this.removeAllOp
        .getRemoveAllEntryData();
    final boolean requiresRegionContext = false;
    for (int i = 0; i < this.removeAllOp.removeAllDataSize; i++) {
      if (!hasTags && removeAllData[i].versionTag != null) {
        hasTags = true;
      }
      VersionTag<?> tag = removeAllData[i].versionTag;
      versionTags.add(tag);
      removeAllData[i].versionTag = null;
      removeAllData[i].toData(out, requiresRegionContext);
      removeAllData[i].versionTag = tag;
    }
    out.writeBoolean(hasTags);
    if (hasTags) {
      InternalDataSerializer.invokeToData(versionTags, out);
    }
  }

  /**
   * @param in
   * @param removeAllSize
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void removeAllFromData(DataInput in, int removeAllSize)
      throws IOException, ClassNotFoundException {
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
    if (this.putAllOp != null) {
      buf.append("putAllDataSize :" + this.putAllOp.putAllDataSize);
    }
    if (this.removeAllOp != null) {
      buf.append("removeAllDataSize :" + this.removeAllOp.removeAllDataSize);
    }
    return buf.toString(); 
  }
}
