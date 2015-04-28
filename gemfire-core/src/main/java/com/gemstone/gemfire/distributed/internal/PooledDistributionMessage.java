/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 * A PooledDistributionMessage passes processing off to a thread that
 * executes on the DistributionManager's getThreadPool().  This is
 * sufficient for messages that don't need to be processed serially
 * with respect to any other message.
 */
public abstract class PooledDistributionMessage extends DistributionMessage {

  @Override
  final public int getProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }

}

