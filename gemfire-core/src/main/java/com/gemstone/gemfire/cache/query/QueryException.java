/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Thrown during by the query engine during parsing or execution.
 * Instances of subclasses are thrown for more specific exceptions.
 * @author Eric Zoerner
 * @since 4.0
 */


public /*abstract*/ class QueryException extends GemFireCheckedException {
private static final long serialVersionUID = 7100830250939955452L;
  
  /**
   * Required for serialization
   */
  public QueryException() {
    
  }

  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public QueryException(String msg, Throwable cause) {
    super(msg, cause);
  }  
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   */
  public QueryException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param cause a Throwable cause of this exception
   */
  public QueryException(Throwable cause) {
    super(cause);
  }  
  
  
}
