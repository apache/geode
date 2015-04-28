/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier;

import com.gemstone.gemfire.GemFireCheckedException;
/**
 * An exception thrown during batch processing.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
// Note that since this class is inside of an internal package,
// we make it extend Exception, thereby making it a checked exception.
public class BatchException extends GemFireCheckedException {
private static final long serialVersionUID = -6707074107791305564L;

  protected int _index;

  /**
   * Required for serialization
   */
  public  BatchException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>RegionQueueException</code> with the
   * specified detail message.
   * @param msg the detail message
   * @param index the index in the batch list where the exception occurred
   */
  public BatchException(String msg, int index) {
    super(msg);
    this._index = index;
  }

  /**
   * Constructor.
   * Creates an instance of <code>RegionQueueException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   * @param index the index in the batch list where the exception occurred
   */
  public BatchException(Throwable cause, int index) {
    super(cause);
    this._index = index;
  }

  /**
   * Answers the index in the batch where the exception occurred
   * @return the index in the batch where the exception occurred
   */
  public int getIndex() {
    return this._index;
  }
}
