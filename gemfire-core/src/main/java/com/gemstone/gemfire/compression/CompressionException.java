/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.compression;

import com.gemstone.gemfire.GemFireException;

/**
 * Wraps specific compression errors for {@link Compressor
 * compressors}.
 * 
 * @author rholmes
 */
public class CompressionException extends GemFireException {

  private static final long serialVersionUID = 4118639654597191235L;

  public CompressionException(String s) {
    super(s);
  }

  public CompressionException(String s, Throwable cause) {
    super(s, cause);
  }

  public CompressionException(Throwable cause) {
    super(cause);
  }
}