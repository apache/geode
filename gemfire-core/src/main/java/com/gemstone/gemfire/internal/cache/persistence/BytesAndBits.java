/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.internal.Version;

/**
 * Used to fetch a record's raw bytes and user bits.
 *
 * @author Darrel Schneider
 * @since prPersistSprint1
 */
public class BytesAndBits {
  private final byte[] data;
  private final byte userBits;
  private Version version;

  public BytesAndBits(byte[] data, byte userBits) {
    this.data = data;
    this.userBits = userBits;
  }

  public final byte[] getBytes() {
    return this.data;
  }
  public final byte getBits() {
    return this.userBits;
  }

  public void setVersion(Version v) {
    this.version = v;
  }

  public Version getVersion() {
    return this.version;
  }
}
