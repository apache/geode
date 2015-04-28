/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.GemFireException;

/**
 * @author dsmith
 *
 */
public class RedundancyAlreadyMetException extends GemFireException {

  public RedundancyAlreadyMetException() {
    super();
  }

  public RedundancyAlreadyMetException(String message, Throwable cause) {
    super(message, cause);
  }

  public RedundancyAlreadyMetException(String message) {
    super(message);
  }

  public RedundancyAlreadyMetException(Throwable cause) {
    super(cause);
  }
}
