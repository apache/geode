/*
 *=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;

/**
 * Extends {@link ResultCollector} interface to provide for methods that are
 * required internally by the product.
 * 
 * @author swale
 */
public interface LocalResultCollector<T, S>
    extends ResultCollector<T, S> {

  /** set any exception during execution of a Function in the collector */
  void setException(Throwable exception);

  /** keep a reference of processor, if required, to avoid it getting GCed */
  void setProcessor(ReplyProcessor21 processor);

  /**
   * get the {@link ReplyProcessor21}, if any, set in the collector by a
   * previous call to {@link #setProcessor(ReplyProcessor21)}
   */
  ReplyProcessor21 getProcessor();
}
