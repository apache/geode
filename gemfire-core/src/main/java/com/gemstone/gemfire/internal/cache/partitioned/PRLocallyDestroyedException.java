/*=========================================================================
 * Copyright (c) 2007, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.cache.DataLocationException;

/**
 * An exception indicating that a PartitionedRegion was found to be Locally
 * Destroyed
 * @author bruce
 * @since 5.1
 *
 */
public class PRLocallyDestroyedException extends DataLocationException {
  private static final long serialVersionUID = -1291911181409686840L;
  public PRLocallyDestroyedException(Throwable cause) {
    super();
    this.initCause(cause);
  }
  
  public PRLocallyDestroyedException(String message) {
    super(message);
  }
  
}
