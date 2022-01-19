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
package org.apache.geode.internal.cache.versions;


/**
 * A in memory representation of a VersionTag that is more compact than the VersionTag.
 *
 * The version tag class has accumulated some temporary flags that make in undesirable to retain a
 * reference for long periods of type.
 *
 * This class is used store version tags that are pending writing to a krf.
 *
 */
public class CompactVersionHolder<T extends VersionSource> implements VersionHolder<T> {
  ////// VersionStamp implementation
  private final T memberID;
  private final short entryVersionLowBytes;
  private final short regionVersionHighBytes;
  private final int regionVersionLowBytes;
  private final byte entryVersionHighByte;
  private final byte distributedSystemId;
  private final long versionTimeStamp;

  public CompactVersionHolder(VersionHolder<T> tag) {
    int eVersion = tag.getEntryVersion();
    entryVersionLowBytes = (short) (eVersion & 0xffff);
    entryVersionHighByte = (byte) ((eVersion & 0xff0000) >> 16);
    regionVersionHighBytes = tag.getRegionVersionHighBytes();
    regionVersionLowBytes = tag.getRegionVersionLowBytes();
    versionTimeStamp = tag.getVersionTimeStamp();
    distributedSystemId = (byte) (tag.getDistributedSystemId() & 0xff);
    memberID = tag.getMemberID();
  }



  @Override
  public int getEntryVersion() {
    return ((entryVersionHighByte << 16) & 0xFF0000) | (entryVersionLowBytes & 0xFFFF);
  }

  @Override
  public long getRegionVersion() {
    return (((long) regionVersionHighBytes) << 32) | (regionVersionLowBytes & 0x00000000FFFFFFFFL);
  }

  @Override
  public long getVersionTimeStamp() {
    return versionTimeStamp;
  }

  @Override
  public T getMemberID() {
    return memberID;
  }

  @Override
  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  public VersionTag asVersionTag() {
    VersionTag tag = VersionTag.create(memberID);
    tag.setEntryVersion(getEntryVersion());
    tag.setRegionVersion(regionVersionHighBytes, regionVersionLowBytes);
    tag.setVersionTimeStamp(getVersionTimeStamp());
    tag.setDistributedSystemId(distributedSystemId);
    return tag;
  }

  /** get rvv internal high byte. Used by region entries for transferring to storage */
  @Override
  public short getRegionVersionHighBytes() {
    return regionVersionHighBytes;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  @Override
  public int getRegionVersionLowBytes() {
    return regionVersionLowBytes;
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("{v").append(getEntryVersion());
    s.append("; rv").append(getRegionVersion());
    if (memberID != null) {
      s.append("; mbr=").append(memberID);
    }
    if (distributedSystemId >= 0) {
      s.append("; ds=").append(distributedSystemId);
    }
    s.append("; time=").append(getVersionTimeStamp());
    s.append("}");
    return s.toString();
  }

}
