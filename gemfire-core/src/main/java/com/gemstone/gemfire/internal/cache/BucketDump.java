/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * This class is used for getting the contents of buckets and then optionally
 * compare them. It may contain the region version vector for the bucket as well
 * as all of the entries.
 * 
 * @author dsmith
 * 
 */
public class BucketDump {
  /**
   * The version vector for this bucket
   */
  private final RegionVersionVector rvv;
  
  /**
   * The contents of the bucket
   */
  private final Map<Object, Object> values;
  
  /**
   * The contents of the bucket
   */
  private final Map<Object, VersionTag> versions;
  
  private final int bucketId;
  
  private final InternalDistributedMember member;

  public BucketDump(int bucketId, InternalDistributedMember member,
      RegionVersionVector rvv, Map<Object, Object> values,
      Map<Object, VersionTag> versions) {
    this.bucketId = bucketId;
    this.member = member;
    this.rvv = rvv;
    this.values = values;
    this.versions = versions;
  }

  public RegionVersionVector getRvv() {
    return rvv;
  }


  public Map<Object, Object> getValues() {
    return values;
  }

  public Map<Object, VersionTag> getVersions() {
    return versions;
  }

  public Map<Object, ArrayList<Object>> getValuesWithVersions() {
    Map<Object, ArrayList<Object>> result = new HashMap<Object, ArrayList<Object>>();

    if (this.values == null) {
      return result;
    }
    for (Entry<Object, Object> e : this.values.entrySet()) {
      ArrayList<Object> list = new ArrayList<Object>();
      list.add(e.getValue());
      list.add(this.versions.get(e.getKey()));
      result.put(e.getKey(), list);
    }
    return result;
  }

  public int getBucketId() {
    return bucketId;
  }

  public InternalDistributedMember getMember() {
    return member;
  }

  @Override
  public String toString()
  {
//    int sz; 
//    synchronized(this) {
//      sz = this.size();
//    }
    return "Bucket id = " + bucketId + " from member = "
        + member
        + ": " + super.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((values == null) ? 0 : values.hashCode());
    result = prime * result + ((versions == null) ? 0 : versions.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BucketDump other = (BucketDump) obj;
    if (values == null) {
      if (other.values != null)
        return false;
    } else if (!values.equals(other.values))
      return false;
    if (versions == null) {
      if (other.versions != null)
        return false;
    } else if (!versions.equals(other.versions))
      return false;
    return true;
  }
}
