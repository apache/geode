/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

/**
 * @author sbawaska
 * @author rdubey
 *
 */
public final class KeyInfo {

  // Rahul: This class should actually be renamed as RoutingInfo or BucketIdInfo
  // since that is exactly what an instance of this class is.

  public static final int UNKNOWN_BUCKET = -1;

  private Object key;
  private Object callbackArg;
  private int bucketId;

  // Rahul: The value field is add since Sqlf Partition resolver also relies on the value
  // part to calculate the routing object if the table is not partitioned on 
  // primary key.
  private final Object value;

  public KeyInfo(Object key, Object value, Object callbackArg) {
    this.key = key;
    this.callbackArg = callbackArg;
    this.bucketId = UNKNOWN_BUCKET;
    this.value =  value;
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
  
  public final Object getKey() {
    return this.key;
  }
  
  public final Object getCallbackArg() {
    return this.callbackArg;
  }
  
  public final Object getValue() {
    return this.value;
  }
  
  public final int getBucketId() {
    return this.bucketId;
  }

  public final void setKey(Object key) {
    this.key = key;
  }

  public final void setBucketId(int bucketId) {
    this.bucketId = bucketId;
  }

  public final void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }

  public String toString() {
    return "(key="+key+",bucketId="+bucketId+")";
  }
}
