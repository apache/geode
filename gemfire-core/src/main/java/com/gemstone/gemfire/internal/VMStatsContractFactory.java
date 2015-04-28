/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.stats50.VMStats50;

/**
 * Factory used to produce an instance of VMStatsContract.
 */
public class VMStatsContractFactory {
  /**
   * Create and return a VMStatsContract.
   */
  public static VMStatsContract create(StatisticsFactory f, long id) {
    VMStatsContract result;
    try {
      result = new VMStats50(f, id);
    } 
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable ignore) {
      // Now that we no longer support 1.4 I'm not sure why we would get here.
      // But just in case other vm vendors don't support mxbeans I've left
      // this logic in that will create a simple VMStats instance.
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      //log.warning("Could not create 5.0 VMStats", ignore);
      // couldn't create the 1.5 version so create the old 1.4 version
      result = new VMStats(f, id);
    }
    return result;
  }
  
  private VMStatsContractFactory() {
    // private so no instances allowed. static methods only
  }
}
