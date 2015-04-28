/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;

/**
 * Implementation of ObjectType
 * @author Eric Zoerner
 * @since 4.0
 */
public class ObjectTypeImpl implements ObjectType, DataSerializableFixedID {
  private static final long serialVersionUID = 3327357282163564784L;
  private Class clazz;
  
  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public ObjectTypeImpl() {
  }
  
  /** Creates a new instance of ObjectTypeImpl */
  public ObjectTypeImpl(Class clazz) {
    this.clazz = clazz;
  }
  
  public ObjectTypeImpl(String className) throws ClassNotFoundException {
    this.clazz = InternalDataSerializer.getCachedClass(className);
  }
  
  public Class resolveClass() {
    return this.clazz;
  }  
  
  public String getSimpleClassName() {
    String cn = this.clazz.getName();
    int i = cn.lastIndexOf('.');
    return i < 0 ? cn : cn.substring(i + 1);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ObjectTypeImpl)) return false;
    return this.clazz == ((ObjectTypeImpl)obj).clazz;
  }
  
  @Override
  public int hashCode() {
    return this.clazz.hashCode();
  }
  
  @Override
  public String toString(){
    return this.clazz.getName();
  }
  
  public boolean isCollectionType() { return false; }
  public boolean isMapType() { return false; }
  public boolean isStructType() { return false; }

  public int getDSFID() {
    return OBJECT_TYPE_IMPL;
  }
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {         
    this.clazz = DataSerializer.readClass(in);
  }     

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeClass(this.clazz, out);    
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
