/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.Version;

/**
 * A persistent event that is stored in the hoplog queue. This class is only used
 * temporarily to copy the data from the HDFSGatewayEventImpl to the persisted
 * record in the file.
 * 
 * @author dsmith
 *
 */
public class SortedHDFSQueuePersistedEvent extends SortedHoplogPersistedEvent implements QueuedPersistentEvent {
  
  
  /**key stored in serialized form*/
  protected byte[] keyBytes = null;
  
  public SortedHDFSQueuePersistedEvent(HDFSGatewayEventImpl in) throws IOException,
  ClassNotFoundException {
    this(in.getSerializedValue(), in.getOperation(), in.getValueIsObject(), in
        .getPossibleDuplicate(), in.getVersionTag(), in.getSerializedKey(), in
        .getCreationTime());
  }

  public SortedHDFSQueuePersistedEvent(Object valueObject, Operation operation,
      byte valueIsObject, boolean possibleDuplicate, VersionTag versionTag,
      byte[] serializedKey, long timestamp) throws ClassNotFoundException, IOException {
    super(valueObject, operation, valueIsObject, possibleDuplicate, versionTag, timestamp);
    this.keyBytes = serializedKey;
    // TODO Auto-generated constructor stub
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeByteArray(this.keyBytes, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.keyBytes = DataSerializer.readByteArray(in);
  }

  @Override
  public void toHoplogEventBytes(DataOutput out) throws IOException {
    super.toData(out);
  }

  public byte[] getRawKey() {
    return this.keyBytes;
  }
  public static int getSizeInBytes(int keySize, int valueSize, VersionTag versionTag) {
    
    int size = SortedHoplogPersistedEvent.getSizeInBytes(keySize, valueSize, versionTag);
    
    size += keySize;
    
    return size;
  }
}
