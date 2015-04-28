/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;


/**
 * This represents the CQ key value pair for the CQ query results.
 *  
 * @author anil
 * @since 6.5
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
