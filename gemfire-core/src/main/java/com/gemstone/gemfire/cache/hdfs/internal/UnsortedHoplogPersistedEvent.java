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
 * A persisted event that is sorted in an unsorted (sequential hoplog). This
 * does not have a version stamp, but just a timestamp for the entry.
 * 
 * This class should only be serialized by calling toData directly, which
 * is why it does not implement DataSerializable.
 * 
 * @author dsmith
 *
 */
public class UnsortedHoplogPersistedEvent extends PersistedEventImpl {
  long timestamp;
  
  

  public UnsortedHoplogPersistedEvent() {
    //for deserialization
  }

  public UnsortedHoplogPersistedEvent(Object value, Operation op,
      byte valueIsObject, boolean isPossibleDuplicate, long timestamp) throws IOException,
      ClassNotFoundException {
    super(value, op, valueIsObject, isPossibleDuplicate, false/*hasVersionTag*/);
    this.timestamp = timestamp;
  }

  @Override
  public long getTimstamp() {
    return timestamp;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeLong(timestamp, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.timestamp = DataSerializer.readLong(in);
  }
  
  public static UnsortedHoplogPersistedEvent fromBytes(byte[] val)
      throws IOException, ClassNotFoundException {
    ByteArrayDataInput in = new ByteArrayDataInput();
    in.initialize(val, null);
    UnsortedHoplogPersistedEvent event = new UnsortedHoplogPersistedEvent();
    event.fromData(in);
    return event;
  }
  
  public void copy(PersistedEventImpl usersValue) {
    super.copy(usersValue);
    this.timestamp = ((UnsortedHoplogPersistedEvent) usersValue).timestamp;
  }
  
  public static int getSizeInBytes(int keySize, int valueSize, VersionTag versionTag) {
    int size = PersistedEventImpl.getSizeInBytes(keySize, valueSize, versionTag);
    
    // size of Timestamp
    size += 8;
    
    return size;
  }
}
