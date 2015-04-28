/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Indicates that a Statistics resource instance with a null StatisticsType
 * should be ignored by the statistics sampler.
 * <p/>
 * Extracted from {@link com.gemstone.gemfire.internal.StatArchiveWriter}.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public class IgnoreResourceException extends GemFireCheckedException {
  private static final long serialVersionUID = 3371071862581873081L;

  /**
   * Creates a new <code>IgnoreResourceException</code> with no detailed message.
   */
  public IgnoreResourceException() {
    super();
  }

  /**
   * Creates a new <code>IgnoreResourceException</code> with the given detail
   * message.
   */
  public IgnoreResourceException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>IgnoreResourceException</code> with the given detail
   * message and cause.
   */
  public IgnoreResourceException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }
  
  /**
   * Creates a new <code>IgnoreResourceException</code> with the given cause and
   * no detail message
   */
  public IgnoreResourceException(Throwable cause) {
    super();
    this.initCause(cause);
  }
}
