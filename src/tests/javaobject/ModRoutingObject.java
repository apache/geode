/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.*;
import java.io.Serializable;
import org.apache.geode.distributed.*;
import org.apache.geode.cache.*;
import org.apache.geode.cache.partition.PartitionRegionHelper;


public class ModRoutingObject implements PartitionResolver, Serializable, Comparable {

  private Object key;
  private int counterValue;
  private int modValue;

  // Takes a String key constructed by NameFactory
  ModRoutingObject(Object key) {
    this.key = key;
    this.counterValue = (Integer)key;

    int numDataStores = 3;
    this.modValue = (int)this.counterValue % numDataStores;
  }

  public Object getKey() {
    return this.key;
  }

  public int getCounterValue() {
    return this.counterValue;
  }

  public long getModValue() {
    return this.modValue;
  }

  public String toString() {
     return counterValue + "_" + modValue;
  }

  // Override equals
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ModRoutingObject)) {
      return false;
    }
    ModRoutingObject o = (ModRoutingObject)obj;
    if (!this.key.equals(o.getKey())) {
      return false;
    }
    if (this.counterValue != o.getCounterValue()) {
      return false;
    }
    if (this.modValue !=  o.getModValue()) {
      return false;
    }
    return true;
  }

  public int hashCode() {
     return this.modValue;
  }

  public String getName() {
    return this.getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation op) {
    return (ModRoutingObject)op.getKey();
  }

  public void close() {
  }

  public int compareTo(Object o) {
    ModRoutingObject mro = (ModRoutingObject)o;
    return (int)(this.counterValue - mro.counterValue);
  }
}


