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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;

import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

public class KeyInfo {

  // Rahul: This class should actually be renamed as RoutingInfo or BucketIdInfo
  // since that is exactly what an instance of this class is.

  public static final int UNKNOWN_BUCKET = -1;

  private Object key;
  private Object callbackArg;
  private int bucketId;

  // The value field is added since a Partition resolver could also rely on the value
  // part to calculate the routing object
  @Retained(ENTRY_EVENT_NEW_VALUE)
  private final Object value;

  public KeyInfo(Object key, Object value, Object callbackArg) {
    this.key = key;
    this.callbackArg = callbackArg;
    this.bucketId = UNKNOWN_BUCKET;
    this.value = value;
  }

  public KeyInfo(Object key, Object callbackArg, int bucketId) {
    this.key = key;
    this.callbackArg = callbackArg;
    this.bucketId = bucketId;
    this.value = null;
  }

  public KeyInfo(KeyInfo keyInfo) {
    this.bucketId = keyInfo.bucketId;
    this.callbackArg = keyInfo.callbackArg;
    this.value = keyInfo.value;
    this.key = keyInfo.key;
  }

  public Object getKey() {
    return this.key;
  }

  public Object getCallbackArg() {
    return this.callbackArg;
  }

  @Unretained(ENTRY_EVENT_NEW_VALUE)
  public Object getValue() {
    return this.value;
  }

  public int getBucketId() {
    return this.bucketId;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  public void setBucketId(int bucketId) {
    this.bucketId = bucketId;
  }

  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }

  public String toString() {
    return "(key=" + key + ",bucketId=" + bucketId + ")";
  }

  /*
   * For Distributed Join Purpose
   */
  public boolean isCheckPrimary() throws UnsupportedOperationInTransactionException {
    return true;
    // throw new UnsupportedOperationInTransactionException(
    // String.format("precommit() operation %s meant for Dist Tx is not supported",
    // "isCheckPrimary"));
  }

  /*
   * For Distributed Join Purpose
   */
  public void setCheckPrimary(boolean checkPrimary)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "setCheckPrimary"));
  }

  public boolean isDistKeyInfo() {
    return false;
  }
}
