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
 * @author Kirk Lund
 * @since 7.0
 */
public class StatisticNotFoundException extends GemFireCheckedException {
  
  private static final long serialVersionUID = -6232790142851058203L;

  /**
   * Creates a new <code>StatisticNotFoundException</code> with no detailed message.
   */
  public StatisticNotFoundException() {
    super();
  }

  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given detail
   * message.
   */
  public StatisticNotFoundException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given detail
   * message and cause.
   */
  public StatisticNotFoundException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }
  
  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given cause and
   * no detail message
   */
  public StatisticNotFoundException(Throwable cause) {
    super();
    this.initCause(cause);
  }
  
}
