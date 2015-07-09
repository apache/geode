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
import com.gemstone.gemfire.internal.cache.versions.VersionTag;


/**
 * A persistent event that is stored in the hoplog queue. This class is only used
 * temporarily to copy the data from the HDFSGatewayEventImpl to the persisted
 * record in the file. 
 * 
 * @author dsmith
 *
 */
public class UnsortedHDFSQueuePersistedEvent extends UnsortedHoplogPersistedEvent implements QueuedPersistentEvent {
  
  /**the bytes of the key for this entry */
  protected byte[] keyBytes = null;
  
  public UnsortedHDFSQueuePersistedEvent(HDFSGatewayEventImpl in) throws IOException,
  ClassNotFoundException {
    super(in.getValue(), in.getOperation(), in.getValueIsObject(), in.getPossibleDuplicate(), 
        in.getVersionTimeStamp() == 0 ? in.getCreationTime() : in.getVersionTimeStamp());
    this.keyBytes = in.getSerializedKey();
    
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
    
    int size = UnsortedHoplogPersistedEvent.getSizeInBytes(keySize, valueSize, versionTag);
    
    size += keySize;
    
    return size;
  }
}
