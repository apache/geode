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


