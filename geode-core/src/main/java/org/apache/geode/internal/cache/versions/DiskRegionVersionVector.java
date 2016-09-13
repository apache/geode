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
package org.apache.geode.internal.cache.versions;

import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.DiskStoreID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A region version vector for regions with persistent data. This region
 * version vector uses the persistent disk store UUID as the member id.
 *
 */
public class DiskRegionVersionVector extends RegionVersionVector<DiskStoreID> {

  /** for deserialization */
  public DiskRegionVersionVector() {
    super();
  }

  public DiskRegionVersionVector(DiskStoreID ownerId, LocalRegion owner) {
    super(ownerId, owner);
  }
  
  public DiskRegionVersionVector(DiskStoreID ownerId) {
    super(ownerId);
  }
  
  

  @Override
  public void recordVersion(DiskStoreID member, long version) {
    //TODO - RVV - This is a temporary check to make sure we get
    //an exception if we try to add a non disk store id to this vector
    super.recordVersion(member, version);
  }


  @Override
  public void recordGCVersion(DiskStoreID mbr, long regionVersion) {
    //TODO - RVV - This is a temporary check to make sure we get
    //an exception if we try to add a non disk store id to this vector
    super.recordGCVersion(mbr, regionVersion);
  }

  public DiskRegionVersionVector(DiskStoreID ownerId,
      ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> vector, long version,
      ConcurrentHashMap<DiskStoreID, Long> gcVersions, long gcVersion,
      boolean singleMember, 
      RegionVersionHolder<DiskStoreID> localExceptions) {
    super(ownerId, vector, version, gcVersions, gcVersion, singleMember,
        localExceptions);
  }

  @Override
  protected RegionVersionVector<DiskStoreID> createCopy(DiskStoreID ownerId,
      ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> vector, long version,
      ConcurrentHashMap<DiskStoreID, Long> gcVersions, long gcVersion,
      boolean singleMember, 
      RegionVersionHolder<DiskStoreID> localExceptions) {
    return new DiskRegionVersionVector(ownerId, vector, version,
        gcVersions, gcVersion, singleMember, localExceptions);
  }

  @Override
  protected void writeMember(DiskStoreID member, DataOutput out) throws IOException {
    out.writeLong(member.getMostSignificantBits());
    out.writeLong(member.getLeastSignificantBits());
    
  }

  @Override
  protected DiskStoreID readMember(DataInput in) throws IOException {
    long mostSignificantBits = in.readLong();
    long leastSignificantBits = in.readLong();
    DiskStoreID member = new DiskStoreID(mostSignificantBits, leastSignificantBits);
    return member;
  }

  @Override
  public int getDSFID() {
    return PERSISTENT_RVV;
  }
}
