/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.nio.ByteBuffer;

/**
 * Used to uniquely identify a conflatable message. If two messages have
 * keys that are equal then the latter message can replace (i.e. conflate)
 * the earlier message.
 * @since 4.2.1
 */
public class ConflationKey {
  private final Object entryKey;
  private final String regionPath;
  private final boolean allowsConflation;
  private ByteBuffer buffer;
  /**
   * Create a new conflation key given its entry key and region
   */
  public ConflationKey(Object entryKey, String regionPath, boolean allowsConflation) {
    this.entryKey = entryKey;
    this.regionPath = regionPath;
    this.allowsConflation = allowsConflation;
  }
  @Override
  public int hashCode() {
    // Note: we intentionally leave out buffer and allowsConflation
    return this.entryKey.hashCode() ^ this.regionPath.hashCode();
  }
  @Override
  public boolean equals(Object obj) {
    // Note: we intentionally leave out buffer and allowsConflation
    boolean result = false;
    if (obj != null) {
      if (obj instanceof ConflationKey) {
        ConflationKey other = (ConflationKey)obj;
        result = this.regionPath.equals(other.regionPath)
          && this.entryKey.equals(other.entryKey);
      }
    }
    return result;
  }
  @Override
  public String toString() {
    StringBuffer result = new StringBuffer(128);
    result.append(this.regionPath).append('/').append(this.entryKey);
    return result.toString();
  }
  public ByteBuffer getBuffer() {
    return this.buffer;
  }
  public void setBuffer(ByteBuffer bb) {
    this.buffer = bb;
  }
  /**
   * Returns true if this key (and the operation that generated it) allows conflation; returns false if it does not support conflation.
   * Retur
   */
  public boolean allowsConflation() {
    return this.allowsConflation;
  }
}
  
