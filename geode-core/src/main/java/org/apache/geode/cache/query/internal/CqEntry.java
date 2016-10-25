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

package org.apache.geode.cache.query.internal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;


/**
 * This represents the CQ key value pair for the CQ query results.
 *  
 * @since GemFire 6.5
 */

public class CqEntry implements DataSerializableFixedID {
  
  private Object key;
  
  private Object value;

  public CqEntry(Object key, Object value){
    this.key = key;
    this.value = value;
  }

  /**
   * Constructor to read serialized data.
   */
  public CqEntry(){
  }

  /**
   * return key
   * @return  Object key
   */
  public Object getKey() {
    return this.key;
  }

  /**
   * return value
   * @return Object value
   */
  public Object getValue() {
    return this.value;
  }

  /**
   * Returns key values as Object[] with key as the
   * first and value as the last element of the array.
   * @return Object[] key, value pair
   */
  public Object[] getKeyValuePair() {
    return new Object[] {key, value};
  }
  
  @Override
  public int hashCode(){
    return this.key.hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    return this.key.equals(((CqEntry)other).getKey());
  }
  
  /* DataSerializableFixedID methods ---------------------------------------- */

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.key = DataSerializer.readObject(in);
    this.value = DataSerializer.readObject(in);
  }
  

  public int getDSFID() {
    return CQ_ENTRY_EVENT;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.value, out);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
