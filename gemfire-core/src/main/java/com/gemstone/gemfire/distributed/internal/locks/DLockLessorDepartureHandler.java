/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Provides hook for handling departure of a lease holder (lessor).
 * <p>
 * Implementation is optional and will be called from 
 * <code>DLockGrantor.handlehandleDepartureOf(Serializable)</code>
 *
 * @author Kirk Lund
 */
public interface DLockLessorDepartureHandler {

  public void handleDepartureOf(InternalDistributedMember owner, DLockGrantor grantor);
  
}

