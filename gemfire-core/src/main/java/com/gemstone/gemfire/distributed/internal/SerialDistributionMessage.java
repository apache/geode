/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 * A SerialDistributionMessage is processed in the order
 * it is generated from a single thread's point of view.
 */
public abstract class SerialDistributionMessage extends DistributionMessage {

  // not "final" because it's overridden in ViewMessage
  @Override
  public int getProcessorType() {
    return DistributionManager.SERIAL_EXECUTOR;
  }


}

