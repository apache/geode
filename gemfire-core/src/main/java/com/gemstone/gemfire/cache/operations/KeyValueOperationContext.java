/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a region operation that requires both key and serialized value
 * for the pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public abstract class KeyValueOperationContext extends KeyOperationContext {

  /**
   * The value of the create/update operation.
   * @since 6.5
   */
  protected Object value;
  
  /**
   * The serialized value of the create/update operation.
   */
  private byte[] serializedValue;

  /**
   * True when the serialized object is a normal object; false when it is a raw
   * byte array.
   */
  private boolean isObject;

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param value
   *                the value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   * @since 6.5
   */
  public KeyValueOperationContext(Object key, Object value,
      boolean isObject) {
    super(key);
    setValue(value,isObject);
    //this.value = value;
    // this.isObject = isObject;
  }

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param value
   *                the value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   * @param postOperation
   *                true if the context is at the time of sending updates
   * @since 6.5
   */
  public KeyValueOperationContext(Object key, Object value,
      boolean isObject, boolean postOperation) {
    super(key, postOperation);
    setValue(value,isObject);
    //this.value = value;
    //this.isObject = isObject;
  }

  /**
   * Get the serialized value for this operation.
   * 
   * @return the serialized value for this operation.
   */
  public byte[] getSerializedValue() {
    return this.serializedValue;
  }

  /**
   * Get the value for this operation.
   * 
   * @return the value for this operation.
   * @since 6.5
   */
  public Object getValue() {
    
    if (serializedValue != null) {
      return serializedValue;
    }
    else {
      return value;
    }
  }
  
  /**
   * Return true when the value is an object and not a raw byte array.
   * 
   * @return true when the value is an object; false when it is a raw byte array
   */
  public boolean isObject() {
    return this.isObject;
  }

  /**
   * Set the serialized value object for this operation.
   * 
   * @param serializedValue
   *                the serialized value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   */
  public void setSerializedValue(byte[] serializedValue, boolean isObject) {
    this.serializedValue = serializedValue;
    this.value = null;
    this.isObject = isObject;
  }
  

  /**
   * Set the result value of the object for this operation.
   * 
   * @param value
   *                the result of this operation; can be a serialized byte array
   *                or a deserialized object
   * @param isObject
   *                true when the value is an object (either serialized or
   *                deserialized); false when it is a raw byte array
   * @since 6.5
   */
  public void setValue(Object value, boolean isObject) {
    if (value instanceof byte[]) {
      setSerializedValue((byte[])value, isObject);
    }
    else {
      this.serializedValue = null;
      this.value = value;
      this.isObject = isObject;
    }
  }
  
}
