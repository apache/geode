/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

import java.io.*;

/**
 * Used to make InterestResultPolicy implement DataSerializableFixedID
 *
 * @author Darrel Schneider
 *
 * @since 5.7 
 */
public final class InterestResultPolicyImpl extends InterestResultPolicy
  implements DataSerializableFixedID {
  private static final long serialVersionUID = -7456596794818237831L;
  /** Should only be called by static field initialization in InterestResultPolicy */
  public InterestResultPolicyImpl(String name) {
    super(name);
  }

  public int getDSFID() {
    return INTEREST_RESULT_POLICY;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeByte(getOrdinal());
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // should never be called since DSFIDFactory.readInterestResultPolicy is used
    throw new UnsupportedOperationException();
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }
}
