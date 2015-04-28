/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 *
 * @author Eric Zoerner
 */
public abstract class HighPriorityDistributionMessage extends DistributionMessage {
  
  @Override
  public int getProcessorType() {
    return DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }
    
}
