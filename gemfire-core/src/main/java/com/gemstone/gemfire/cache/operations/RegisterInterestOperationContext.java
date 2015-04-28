/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import com.gemstone.gemfire.cache.InterestResultPolicy;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#REGISTER_INTEREST} region operation for
 * the pre-operation case.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class RegisterInterestOperationContext extends InterestOperationContext {

  /** The <code>InterestResultPolicy</code> for this list of keys. */
  private InterestResultPolicy policy;

  /**
   * Constructor for the register interest operation.
   * 
   * @param key
   *                the key or list of keys being registered
   * @param interestType
   *                the <code>InterestType</code> of the register request
   * @param policy
   *                the <code>InterestResultPolicy</code> of the register
   *                request
   */
  public RegisterInterestOperationContext(Object key,
      InterestType interestType, InterestResultPolicy policy) {
    super(key, interestType);
    this.policy = policy;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.REGISTER_INTEREST</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.REGISTER_INTEREST;
  }

  /**
   * Get the <code>InterestResultPolicy</code> of this register/unregister
   * operation.
   * 
   * @return the <code>InterestResultPolicy</code> of this request.
   */
  public InterestResultPolicy getInterestResultPolicy() {
    return this.policy;
  }

}
