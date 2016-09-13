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


/**
 * A in memory representation of a VersionTag that is more compact than the VersionTag.
 * 
 * The version tag class has accumulated some temporary flags that make in undesirable
 * to retain a reference for long periods of type.
 * 
 * This class is used store version tags that are pending writing to a krf.
 *
 */
public class CompactVersionHolder<T extends VersionSource> implements VersionHolder<T> {
  //////VersionStamp implementation
  private final T memberID;
  private final short entryVersionLowBytes;
  private final short regionVersionHighBytes;
  private final int regionVersionLowBytes;
  private final byte entryVersionHighByte;
  private final byte distributedSystemId;
  private final long versionTimeStamp;
  
  public CompactVersionHolder(VersionHolder<T> tag) {
    int eVersion = tag.getEntryVersion();
    this.entryVersionLowBytes = (short)(eVersion & 0xffff);
    this.entryVersionHighByte = (byte)((eVersion & 0xff0000) >> 16);
    this.regionVersionHighBytes = tag.getRegionVersionHighBytes();
    this.regionVersionLowBytes = tag.getRegionVersionLowBytes();
    this.versionTimeStamp = tag.getVersionTimeStamp();
    this.distributedSystemId = (byte)(tag.getDistributedSystemId() & 0xff);
    this.memberID = tag.getMemberID();
  }



  public int getEntryVersion() {
    return ((entryVersionHighByte << 16) & 0xFF0000) | (entryVersionLowBytes & 0xFFFF);
  }

  public long getRegionVersion() {
    return (((long)regionVersionHighBytes) << 32) | (regionVersionLowBytes & 0x00000000FFFFFFFFL);  
  }

  public long getVersionTimeStamp() {
    return versionTimeStamp;
  }

  public T getMemberID() {
    return this.memberID;
  }
  public int getDistributedSystemId() {
    return this.distributedSystemId;
  }

  public VersionTag asVersionTag() {
    VersionTag tag = VersionTag.create(memberID);
    tag.setEntryVersion(getEntryVersion());
    tag.setRegionVersion(this.regionVersionHighBytes, this.regionVersionLowBytes);
    tag.setVersionTimeStamp(getVersionTimeStamp());
    tag.setDistributedSystemId(this.distributedSystemId);
    return tag;
  }
  
  /** get rvv internal high byte.  Used by region entries for transferring to storage */
  public short getRegionVersionHighBytes() {
    return this.regionVersionHighBytes;
  }
  
  /** get rvv internal low bytes.  Used by region entries for transferring to storage */
  public int getRegionVersionLowBytes() {
    return this.regionVersionLowBytes;
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("{v").append(getEntryVersion());
    s.append("; rv").append(getRegionVersion());
    if (this.memberID != null) {
      s.append("; mbr=").append(this.memberID);
    }
    if (this.distributedSystemId >= 0) {
      s.append("; ds=").append(this.distributedSystemId);
    }
    s.append("; time=").append(getVersionTimeStamp());
    s.append("}");
    return s.toString();
  }

}
