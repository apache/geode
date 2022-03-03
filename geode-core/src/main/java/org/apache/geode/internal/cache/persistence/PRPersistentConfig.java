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
package org.apache.geode.internal.cache.persistence;

public class PRPersistentConfig {
  private final int totalNumBuckets;
  private final String colocatedWith;

  public PRPersistentConfig(int totalNumBuckets, String colocatedWith) {
    super();
    this.totalNumBuckets = totalNumBuckets;
    this.colocatedWith = colocatedWith;
  }

  public int getTotalNumBuckets() {
    return totalNumBuckets;
  }

  public String getColocatedWith() {
    return colocatedWith;
  }

  @Override
  public String toString() {
    return "numBuckets=" + totalNumBuckets + ", colocatedWith=" + colocatedWith;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((colocatedWith == null) ? 0 : colocatedWith.hashCode());
    result = prime * result + totalNumBuckets;
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
    if (!(obj instanceof PRPersistentConfig)) {
      return false;
    }
    PRPersistentConfig other = (PRPersistentConfig) obj;
    if (colocatedWith == null) {
      if (other.colocatedWith != null) {
        return false;
      }
    } else if (!colocatedWith.equals(other.colocatedWith)) {
      return false;
    }
    return totalNumBuckets == other.totalNumBuckets;
  }
}
