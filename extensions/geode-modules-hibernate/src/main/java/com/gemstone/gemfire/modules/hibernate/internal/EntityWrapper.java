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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class EntityWrapper implements DataSerializable {

  private static final long serialVersionUID = 8616754027252339041L;

  private Object entity;

  private long version;

  public EntityWrapper(Object entity, long version) {
    this.entity = entity;
    this.version = version;
  }

  /**
   * for {@link DataSerializer}
   */
  public EntityWrapper() {
  }
  
  public long getVersion() {
    return version;
  }

  public Object getEntity() {
    return entity;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EntityWrapper) {
      EntityWrapper other = (EntityWrapper)obj;
      if (this.version == other.version) {
        //CacheEntry does not override equals, hence cannot be used in this comparison
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(this.version).hashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("EntityWrapper@" + System.identityHashCode(this))
        .append(" Entity:" + this.entity).append(" version:" + this.version)
        .toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.version);
    DataSerializer.writeObject(this.entity, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.version = in.readLong();
    this.entity = DataSerializer.readObject(in);
  }
}
