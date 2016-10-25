/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.partitioned;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.logging.LogService;

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
    if (stopper.isCancelInProgress()) {
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
