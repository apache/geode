/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.exception;

import com.gemstone.gemfire.pdx.JSONFormatterException;

/**
 * Indicates that incorrect JSON document encountered while processing it.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */

@SuppressWarnings("unused")
public class MalformedJsonException extends RuntimeException {

  public MalformedJsonException() {
  }

  public MalformedJsonException(String message) {
    super(message);
  }
 
  public MalformedJsonException(String message, JSONFormatterException jfe) {
    super(message, jfe);
  }
  
  public MalformedJsonException(Throwable cause) {
    super(cause);
  }

  public MalformedJsonException(String message, Throwable cause) {
    super(message, cause);
  }

}
