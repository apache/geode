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
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * The member that originated an update that is stored in the version
 * information of a region entry, or in the regions version vector.
 * 
 * A VersionMember could either be an InternalDistributedMember (for an in
 * memory region), or a UUID (for a persistent region).
 * 
 * VersionMembers should implement equals and hashcode.
 * 
 *
 */
public interface VersionSource<T> extends DataSerializableFixedID, Comparable<T> {
  
  public void writeEssentialData(DataOutput out) throws IOException;
  
  public int getSizeInBytes();

}
