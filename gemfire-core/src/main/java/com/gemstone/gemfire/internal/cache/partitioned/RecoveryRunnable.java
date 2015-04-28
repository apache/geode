/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.logging.LogService;

public abstract class RecoveryRunnable implements Runnable {
  private static final Logger logger = LogService.getLogger();
  
  protected final PRHARedundancyProvider redundancyProvider;

  /**
   * @param prhaRedundancyProvider
   */
  public RecoveryRunnable(PRHARedundancyProvider prhaRedundancyProvider) {
    redundancyProvider = prhaRedundancyProvider;
  }

  private volatile Throwable failure;
  
  public abstract void run2();

  public void checkFailure() {
    if(failure != null) {
      if( failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      } else {
        throw new InternalGemFireError("Failure during bucket recovery ", failure);
      }
    }
  }

  public void run()
  {
    CancelCriterion stopper = redundancyProvider.prRegion
        .getGemFireCache().getDistributedSystem().getCancelCriterion();
    DistributedSystem.setThreadsSocketPolicy(true /* conserve sockets */);
    SystemFailure.checkFailure();
    if (stopper.cancelInProgress() != null) {
      return;
    }
    try {
      run2();
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (logger.isDebugEnabled()) {
        logger.debug("Unexpected exception in PR redundancy recovery", t);
      }
      failure = t;
    }

  }
}