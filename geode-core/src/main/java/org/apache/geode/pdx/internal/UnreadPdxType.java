/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.pdx.internal;

/**
 * Just like a PdxType but also keeps track of the unreadFields. This is used when we deserialize a
 * pdx and the blob we are deserializing contains fields that our local version does not read. Note
 * that instances of the class are only kept locally so I didn't add code to serialize
 * unreadFieldIndexes.
 *
 * @since GemFire 6.6
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
    return unreadFieldIndexes;
  }

  public PdxType getSerializedType() {
    return serializedType;
  }

  public void setSerializedType(PdxType t) {
    serializedType = t;
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

  // public String toString() {
  // return "unreadFieldIdxs=" + java.util.Arrays.toString(this.unreadFieldIndexes) +
  // super.toString();
  // }
}
