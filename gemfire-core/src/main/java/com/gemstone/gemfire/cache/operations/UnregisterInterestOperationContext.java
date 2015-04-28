/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#UNREGISTER_INTEREST} region operation for
 * the pre-operation case.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class UnregisterInterestOperationContext extends InterestOperationContext {

  /**
   * Constructor for the unregister interest operation.
   * 
   * @param key
   *                the key or list of keys being unregistered
   * @param interestType
   *                the <code>InterestType</code> of the unregister request
   */
  public UnregisterInterestOperationContext(Object key,
      InterestType interestType) {
    super(key, interestType);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.UNREGISTER_INTEREST</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.UNREGISTER_INTEREST;
  }

}
