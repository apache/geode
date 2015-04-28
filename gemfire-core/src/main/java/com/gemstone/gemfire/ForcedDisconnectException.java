/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>ForcedDisconnectException</code> is thrown when a GemFire
 * application is removed from the distributed system due to membership
 * constraints such as network partition detection.
 * 
 * @since 5.7
 */
public class ForcedDisconnectException extends CancelException {
private static final long serialVersionUID = 4977003259880566257L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemConnectException</code>.
   */
  public ForcedDisconnectException(String message) {
    super(message);
  }
  
  public ForcedDisconnectException(String message, Throwable cause) {
    super(message, cause);
  }
}
