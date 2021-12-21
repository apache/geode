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
package org.apache.geode.internal.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * This class is used for getting the contents of buckets and then optionally compare them. It may
 * contain the region version vector for the bucket as well as all of the entries.
 *
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

  public BucketDump(int bucketId, InternalDistributedMember member, RegionVersionVector rvv,
      Map<Object, Object> values, Map<Object, VersionTag> versions) {
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
    Map<Object, ArrayList<Object>> result = new HashMap<>();

    if (values == null) {
      return result;
    }
    for (Entry<Object, Object> e : values.entrySet()) {
      ArrayList<Object> list = new ArrayList<>();
      list.add(e.getValue());
      list.add(versions.get(e.getKey()));
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
  public String toString() {
    // int sz;
    // synchronized(this) {
    // sz = this.size();
    // }
    return "Bucket id = " + bucketId + " from member = " + member + ": " + super.toString();
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BucketDump other = (BucketDump) obj;
    if (values == null) {
      if (other.values != null) {
        return false;
      }
    } else if (!values.equals(other.values)) {
      return false;
    }
    if (versions == null) {
      return other.versions == null;
    } else
      return versions.equals(other.versions);
  }
}
