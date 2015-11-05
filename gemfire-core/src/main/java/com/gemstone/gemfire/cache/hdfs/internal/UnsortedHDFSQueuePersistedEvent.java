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
