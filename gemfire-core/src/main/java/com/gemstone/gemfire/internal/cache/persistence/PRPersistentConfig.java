/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

/**
 * @author dsmith
 *
 */
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
    result = prime * result
        + ((colocatedWith == null) ? 0 : colocatedWith.hashCode());
    result = prime * result + totalNumBuckets;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof PRPersistentConfig))
      return false;
    PRPersistentConfig other = (PRPersistentConfig) obj;
    if (colocatedWith == null) {
      if (other.colocatedWith != null)
        return false;
    } else if (!colocatedWith.equals(other.colocatedWith))
      return false;
    if (totalNumBuckets != other.totalNumBuckets)
      return false;
    return true;
  }
}
