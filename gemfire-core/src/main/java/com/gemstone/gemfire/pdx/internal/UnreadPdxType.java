/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

/**
 * Just like a PdxType but also keeps track of the unreadFields.
 * This is used when we deserialize a pdx and the blob we are deserializing
 * contains fields that our local version does not read.
 * Note that instances of the class are only kept locally so I didn't add code
 * to serialize unreadFieldIndexes.
 * 
 * @author darrel
 * @since 6.6
 */
public class UnreadPdxType extends PdxType {

  private static final long serialVersionUID = 582651859847937174L;

  private final int[] unreadFieldIndexes;
  /**
   * Null until this unread type is finally serialized for the first time.
   */
  private PdxType serializedType;
  
  public UnreadPdxType(PdxType pdxType, int[] unreadFieldIndexes) {
    super(pdxType);
    this.unreadFieldIndexes = unreadFieldIndexes;
  }

  public int[] getUnreadFieldIndexes() {
    return this.unreadFieldIndexes;
  }

  public PdxType getSerializedType() {
    return this.serializedType;
  }

  public void setSerializedType(PdxType t) {
    this.serializedType = t;
  }  

  @Override
  public int hashCode() {
    // super hashCode is all we need
    // overriding to make this clear
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    // No need to compare unreadFieldIndexes
    // overriding to make this clear
    return super.equals(other);
  }

//   public String toString() {
//     return "unreadFieldIdxs=" + java.util.Arrays.toString(this.unreadFieldIndexes) + super.toString();
//   }
}
