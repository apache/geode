package com.gemstone.gemfire.internal.cache.tx;

import com.gemstone.gemfire.internal.cache.KeyInfo;

/*
 * Created especially for Distributed Tx Purpose
 */
public class DistTxKeyInfo extends KeyInfo {
  boolean checkPrimary = true;
  
  public DistTxKeyInfo(Object key, Object value, Object callbackArg, Integer bucketId) {
    super(key, value, callbackArg);
    setBucketId(bucketId);
  }
  
  public DistTxKeyInfo(DistTxKeyInfo other) {
    super(other);
    this.checkPrimary = other.checkPrimary;
  }
  
  public DistTxKeyInfo(KeyInfo other) {
    super(other);
  }
  
  @Override
  public boolean isCheckPrimary() {
    return checkPrimary;
  }

  @Override
  public void setCheckPrimary(boolean checkPrimary) {
    this.checkPrimary = checkPrimary;
  }
  
  public boolean isDistKeyInfo() {
    return true;
  }
}