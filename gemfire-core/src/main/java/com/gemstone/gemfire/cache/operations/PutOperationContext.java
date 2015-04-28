/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import com.gemstone.gemfire.cache.Region;

/**
 * Encapsulates an {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#PUT} region operation having both key
 * and value objects for for both the pre-operation case and for post-operation
 * updates.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class PutOperationContext extends KeyValueOperationContext {

  /**
   * Indicates that it is not known whether the operation results in a create or
   * in an update. The authorization callback should explicitly use the
   * {@link Region#containsKey} method to determine it when required.
   */
  public static final byte UNKNOWN = 0;

  /**
   * Indicates that the operation results in a create of the key.
   */
  public static final byte CREATE = 1;

  /**
   * Indicates that the operation results in an update of the key.
   */
  public static final byte UPDATE = 2;

  /**
   * Flag to indicate whether the current operation is a create or update or
   * unknown.
   */
  private byte opType;

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
  public PutOperationContext(Object key,Object value,
      boolean isObject) {
    super(key, value, isObject);
    this.opType = UNKNOWN;
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
  public PutOperationContext(Object key, Object value,
      boolean isObject, boolean postOperation) {
    super(key, value, isObject, postOperation);
    this.opType = UNKNOWN;
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
   * @param opType
   *                flag to indicate whether the operation is create/update or
   *                unknown
   * @param isPostOperation
   *                true if the context is at the time of sending updates
   * @since 6.5
   */
  public PutOperationContext(Object key, Object value,
      boolean isObject, byte opType, boolean isPostOperation) {
    super(key, value, isObject, isPostOperation);
    this.opType = opType;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.PUT</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.PUT;
  }

  /**
   * Return whether the operation is a create or update or unknown.
   * 
   * The user should check against {@link PutOperationContext#CREATE},
   * {@link PutOperationContext#UPDATE}, {@link PutOperationContext#UNKNOWN}.
   * For the {@link PutOperationContext#UNKNOWN} case, the authorization
   * callback should explicitly invoke {@link Region#containsKey} to determine
   * if it is create or update when required.
   * 
   * @return one of {@link PutOperationContext#CREATE},
   *         {@link PutOperationContext#UPDATE},
   *         {@link PutOperationContext#UNKNOWN}
   */
  public byte getOpType() {
    return this.opType;
  }

}
