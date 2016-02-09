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
package com.gemstone.gemfire.modules.hibernate.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.hibernate.cache.CacheKey;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * wraps {@link CacheKey}, and implements equals and
 * hashCode. This is required for register interest
 * operation/prefetching
 * @author sbawaska
 *
 */
public class KeyWrapper implements DataSerializable {
  
  private Serializable key;
  private String entityName;
  
  private static final String separator = "#";
  
  public KeyWrapper() {
  }
  
  public KeyWrapper(Object p_key) {
    if (p_key instanceof String) {
      String stringKey = (String)p_key;
      this.key = stringKey.substring(stringKey.indexOf(separator)+1);
      this.entityName = stringKey.substring(0, stringKey.indexOf(separator));
    } else {
      CacheKey cacheKey = (CacheKey)p_key;
      this.key = cacheKey.getKey();
      this.entityName = cacheKey.getEntityOrRoleName();
    }
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KeyWrapper) {
      KeyWrapper other = (KeyWrapper)obj;
      if (this.key.toString().equals(other.key.toString())
          && this.entityName.equals(other.entityName)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.key.toString().hashCode() + this.entityName.hashCode();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.entityName).append(separator).append(this.key);
    return sb.toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.key, out);
    out.writeUTF(this.entityName);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.key = DataSerializer.readObject(in);
    this.entityName = in.readUTF();
  }
}
