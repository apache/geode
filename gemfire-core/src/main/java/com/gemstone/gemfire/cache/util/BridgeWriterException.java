/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.CacheWriterException;

/**
 * An exception that is thrown by a {@link BridgeWriter} when a
 * problem occurs when communicating with a bridge server.
 *
 * @author David Whitlock
 * @since 3.5.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class BridgeWriterException extends CacheWriterException {
private static final long serialVersionUID = -295001316745954159L;

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * message. 
   */
  public BridgeWriterException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * message and cause.
   */
  public BridgeWriterException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>BridgeWriterException</code> with the given
   * cause.
   */
  public BridgeWriterException(Throwable cause) {
    super(cause);
  }

}
