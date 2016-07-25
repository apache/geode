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

package com.gemstone.gemfire.cache.query.internal.types;

import java.io.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.DataSerializer;


/**
 * Implementation of CollectionType
 * @since GemFire 4.0
 */
public final class MapTypeImpl extends CollectionTypeImpl
implements MapType {
  private static final long serialVersionUID = -705688605389537058L;
  private ObjectType keyType;
  
  /**
  * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public MapTypeImpl() {
  }

  /** Creates a new instance of ObjectTypeImpl */
  public MapTypeImpl(Class clazz, ObjectType keyType, ObjectType valueType) {
    super(clazz, valueType);
    this.keyType = keyType;
  }
  
  public MapTypeImpl(String className, ObjectType keyType, ObjectType valueType)
  throws ClassNotFoundException {
    super(className, valueType);
    this.keyType = keyType;
  }
  
  @Override  
  public boolean equals(Object obj) {
    return super.equals(obj) &&
            (obj instanceof MapTypeImpl) &&
            this.keyType.equals(((MapTypeImpl)obj).keyType);
  }
  
  @Override  
  public int hashCode() {
    return super.hashCode() ^ this.keyType.hashCode();
  }
  
  @Override  
  public String toString(){
    return resolveClass().getName() +
            "<key:" + this.keyType.resolveClass().getName() +
            ",value:" + getElementType().resolveClass().getName() + ">";
  }
  
  @Override  
  public boolean isMapType() { return true; }

  public ObjectType getKeyType() {
    return this.keyType;
  }

  public StructType getEntryType() {
    ObjectType[] fieldTypes = new ObjectType[] { this.keyType, getElementType() };
    return new StructTypeImpl( new String[] { "key", "value" }, fieldTypes);
  }
  
  @Override  
  public int getDSFID() {
    return MAP_TYPE_IMPL;
  }

  @Override  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.keyType = (ObjectType)DataSerializer.readObject(in);
  }
  
  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.keyType, out);
  }
}
