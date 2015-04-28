/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.types;

import java.io.*;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.DataSerializer;


/**
 * Implementation of CollectionType
 * @author Eric Zoerner
 * @since 4.0
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
