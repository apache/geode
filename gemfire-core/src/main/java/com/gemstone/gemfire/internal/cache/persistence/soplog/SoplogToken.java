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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.cache.EntryBits;

/**
 * Defines serialized tokens for soplogs. 
 */
public enum SoplogToken {
  
  /** indicates the serialized value is a wildcard compares equal to any other key */
  WILDCARD( DSCODE.WILDCARD ),
  
  /** indicates the serialized value is a tombstone of a deleted key */ 
  TOMBSTONE( EntryBits.setTombstone((byte)0, true) ),

  /** indicates the serialized value is a invalid token*/
  INVALID( EntryBits.setInvalid((byte)0, true) ),

  /** indicates the serialized tombstone has been garbage collected*/
  REMOVED_PHASE2( EntryBits.setLocalInvalid((byte)0, true) ),
  
  /** indicates the value is serialized */
  SERIALIZED( EntryBits.setSerialized((byte)0, true) );

  /** the serialized form of the token */
  private final byte val;
  
  private SoplogToken(byte val) {
    this.val = val;
  }
  
  @Override
  public String toString() {
    return super.toString()+" byte:"+val;
  }

  /**
   * Returns the serialized form of the token.
   * @return the byte
   */
  public byte toByte() {
    return val;
  }
  
  /**
   * Returns true if either of the serialized objects is a wildcard.
   * 
   * @param b1 the first object
   * @param off1 the first offset
   * @param b2 the second object
   * @param off2 the second object
   * @return true if a wildcard
   */
  public static boolean isWildcard(byte[] b1, int off1, byte[] b2, int off2) {
    return b1[off1] == DSCODE.WILDCARD || b2[off2] == DSCODE.WILDCARD;
  }
  
  /**
   * Returns true if the serialized object is a tombstone.
   * 
   * @param b the magic entry type byte
   * @return true if a tombstone
   */
  public static boolean isTombstone(byte b) {
    return EntryBits.isTombstone(b);
  }

  /**
   * Returns true if the serialized object is an invalid token.
   * 
   * @param b the magic entry type byte
   * @return true if invalid
   */
  public static boolean isInvalid(byte b) {
    return EntryBits.isInvalid(b);
  }

  /**
   * Returns true if the serialized tombstone was garbage collected
   * 
   * @param b the magic entry type byte
   * @return true if RemovedPhase2
   */
  public static boolean isRemovedPhase2(byte b) {
    return EntryBits.isLocalInvalid(b);
  }

  /**
   * Returns true if the serialized object is not any token
   * 
   *@param b the magic entry type byte
   * @return true if not any token
   */
  public static boolean isSerialized(byte b) {
    return EntryBits.isSerialized(b);
  }
}


