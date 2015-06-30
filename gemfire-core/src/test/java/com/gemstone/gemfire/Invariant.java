/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire;

/**
 * Interface used for testing an invariant
 * @author  ericz
 *
 */
public interface Invariant {
  /**
   * @return error message, or null if verification passes
   */
  public InvariantResult verify(Object obj);  
}



