/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * A Token representing null.
 * @author Tejas Nomulwar
 * @since cedar
 *
 */
public class NullToken implements DataSerializableFixedID, Comparable {

  public NullToken() {
    Support.assertState(IndexManager.NULL == null,
        "NULL constant already instantiated");
  }

  @Override
  public int compareTo(Object o) {
    // A Null token is equal to other Null token and less than any other object
    // except UNDEFINED
    if (o.equals(this)) {
      return 0;
    } else if (o.equals(QueryService.UNDEFINED)) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof NullToken;
  }

  @Override
  public int getDSFID() {
    return NULL_TOKEN;
  }

  @Override
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
