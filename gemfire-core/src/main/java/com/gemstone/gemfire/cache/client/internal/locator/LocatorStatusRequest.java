/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.cache.client.internal.locator;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * The LocatorStatusRequest class...
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest
 * @see com.gemstone.gemfire.internal.DataSerializableFixedID
 * @since 7.0
 */
public class LocatorStatusRequest extends ServerLocationRequest {

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_STATUS_REQUEST;
  }

}
