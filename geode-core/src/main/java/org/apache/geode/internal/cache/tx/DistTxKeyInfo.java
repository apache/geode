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
package org.apache.geode.internal.cache.tx;

import org.apache.geode.internal.cache.KeyInfo;

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
    checkPrimary = other.checkPrimary;
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

  @Override
  public boolean isDistKeyInfo() {
    return true;
  }
}
