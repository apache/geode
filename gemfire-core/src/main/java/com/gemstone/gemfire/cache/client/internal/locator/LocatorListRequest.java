/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import com.gemstone.gemfire.internal.DataSerializableFixedID;


/**
 * @author dsmith
 *
 */
public class LocatorListRequest extends ServerLocationRequest {

  @Override
  public String toString() {
    return "LocatorListRequest{group=" + getServerGroup() + "}";
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_LIST_REQUEST;
  }
}
