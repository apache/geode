/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A region version vector for regions with persistent data. This region
 * version vector uses the persistent disk store UUID as the member id.
 * @author dsmith
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
