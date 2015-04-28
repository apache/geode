/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.operations;

import com.gemstone.gemfire.cache.operations.KeyOperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#CONTAINS_KEY} region operation having the
 * key object for the pre-operation case.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class ContainsKeyOperationContext extends KeyOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public ContainsKeyOperationContext(Object key) {
    super(key);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.CONTAINS_KEY</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.CONTAINS_KEY;
  }

}
