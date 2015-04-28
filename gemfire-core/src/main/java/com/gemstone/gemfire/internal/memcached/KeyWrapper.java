/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Since byte[] cannot be used as keys in a Region/Map, instances of this
 * class are used. Instances of this class encapsulate byte[] keys and
 * override equals and hashCode to base them on contents on byte[].
 * 
 * @author sbawaska
 */
public class KeyWrapper implements DataSerializable {

  private static final long serialVersionUID = -3241981993525734772L;

  /**
   * the key being wrapped
   */
  private byte[] key;

  public KeyWrapper() {
  }

  private KeyWrapper(byte[] key) {
    this.key = key;
  }

  /**
   * This method should be used to obtain instances of KeyWrapper.
   * @param key the key to wrap
   * @return an instance of KeyWrapper that can be used as a key in Region/Map
   */
  public static KeyWrapper getWrappedKey(byte[] key) {
    return new KeyWrapper(key);
  }

  public byte[] getKey() {
    return this.key;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(this.key, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.key = DataSerializer.readByteArray(in);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KeyWrapper) {
      KeyWrapper other = (KeyWrapper) obj;
      return Arrays.equals(this.key, other.key);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.key);
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append(getClass().getCanonicalName()).append("@").append(System.identityHashCode(this));
    str.append(" key:").append(Arrays.toString(this.key));
    str.append(" hashCode:").append(hashCode());
    return str.toString();
  }
}
