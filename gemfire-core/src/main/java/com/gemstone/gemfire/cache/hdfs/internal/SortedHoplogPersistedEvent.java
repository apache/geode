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
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * A persistent event that is stored in a sorted hoplog. In addition
 * to the fields of PersistentEventImpl, this event has a version tag.
 * 
 * This class should only be serialized by directly calling toData,
 * which is why it does not implement DataSerializable
 * 
 * @author dsmith
 */
public class SortedHoplogPersistedEvent extends PersistedEventImpl {
  /** version tag for concurrency checks */
  protected VersionTag versionTag;

  /** timestamp of the event. Used when version checks are disabled*/
  protected long timestamp;

  public SortedHoplogPersistedEvent(Object valueObject, Operation operation,
      byte valueIsObject, boolean possibleDuplicate, VersionTag tag, long timestamp) throws ClassNotFoundException, IOException {
    super(valueObject, operation, valueIsObject, possibleDuplicate, tag != null);
    this.versionTag = tag;
    this.timestamp = timestamp;
  }

  public SortedHoplogPersistedEvent() {
    //for deserialization
  }

  @Override
  public long getTimstamp() {
    return versionTag == null ? timestamp : versionTag.getVersionTimeStamp();
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    if (versionTag == null) {
      out.writeLong(timestamp);
    } else {
      //TODO optimize these
      DataSerializer.writeObject(this.versionTag, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    if (hasVersionTag()) {
      this.versionTag = (VersionTag)DataSerializer.readObject(in);
    } else {
      this.timestamp = in.readLong();
    }
  }
  
  /**
   * @return the concurrency versioning tag for this event, if any
   */
  public VersionTag getVersionTag() {
    return this.versionTag;
  }
  
  public static SortedHoplogPersistedEvent fromBytes(byte[] val)
      throws IOException, ClassNotFoundException {
    ByteArrayDataInput in = new ByteArrayDataInput();
    in.initialize(val, null);
    SortedHoplogPersistedEvent event = new SortedHoplogPersistedEvent();
    event.fromData(in);
    return event;
  }
  
  public void copy(PersistedEventImpl usersValue) {
    super.copy(usersValue);
    this.versionTag = ((SortedHoplogPersistedEvent) usersValue).versionTag;
    this.timestamp = ((SortedHoplogPersistedEvent) usersValue).timestamp;
  }
  
  public static int getSizeInBytes(int keySize, int valueSize, VersionTag versionTag) {
    int size = PersistedEventImpl.getSizeInBytes(keySize, valueSize, versionTag);
    
    if (versionTag != null) {
      size +=  versionTag.getSizeInBytes();
    } else {
      // size of Timestamp
      size += 8;
    }
    
    return size;
  }
}
