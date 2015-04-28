/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dsmith
 *
 */
public enum PersistentMemberState {
  OFFLINE,
  ONLINE,
  EQUAL,
  REVOKED;
  
  public static PersistentMemberState fromData(DataInput in) throws IOException {
    byte ordinal = in.readByte();
    return PersistentMemberState.values()[ordinal];
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeByte((byte) this.ordinal());
  }
}
