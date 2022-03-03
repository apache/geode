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
package org.apache.geode.distributed.internal;

/**
 * FlowControlParams are used to represent mcast-flow-control parameters for a DistributedSystem.
 * Instances of this class are used in DistributionConfig to hold the relevant settings, which
 * are<br>
 * - byteAllowance, the number of bytes that can be transmitted to another process without a
 * recharge<br>
 * - rechargeThreshold, the ratio of (initial byteAllowance)/(current byteAllowance) that causes a
 * process to send a recharge message<br>
 * - rechargeBlockMs, the maximum wait time for a recharge before explicitly asking for a recharge
 * <p>
 * The byteAllowance and rechargeBlockMs settings are used in hashcode calculations, and should not
 * be changed if the hashcode of a FlowControlParams needs to remain invariant.
 *
 * @since GemFire 5.0
 */

public class FlowControlParams implements java.io.Serializable {
  private static final long serialVersionUID = 7322447678546893647L;

  private int byteAllowance;
  private float rechargeThreshold;
  private int rechargeBlockMs;

  /** for serialization use only */
  public FlowControlParams() {}

  /** instantiate a FlowControlParams */
  public FlowControlParams(int byteAllowance, float rechargeThreshold, int rechargeBlockMs) {
    this.byteAllowance = byteAllowance;
    this.rechargeThreshold = rechargeThreshold;
    this.rechargeBlockMs = rechargeBlockMs;
  }

  /** return a string representation of this object */
  @Override
  public String toString() {
    return ("" + byteAllowance + ", " + rechargeThreshold + ", " + rechargeBlockMs);
  }

  /** returns the byteAllowance setting */
  public int getByteAllowance() {
    return byteAllowance;
  }

  /** returns the rechargeThreshold setting */
  public float getRechargeThreshold() {
    return rechargeThreshold;
  }

  /** returns the rechargeBlockMs setting */
  public int getRechargeBlockMs() {
    return rechargeBlockMs;
  }

  /** sets the byteAllowance */
  public void setByteAllowance(int value) {
    byteAllowance = value;
  }

  /** sets the rechargeThreshold */
  public void setRechargeThreshold(float value) {
    rechargeThreshold = value;
  }

  /** sets the rechargeBlockMs */
  public void setRechargeBlockMs(int value) {
    rechargeBlockMs = value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof FlowControlParams) {
      FlowControlParams other = (FlowControlParams) obj;
      return (byteAllowance == other.byteAllowance)
          && (rechargeThreshold == other.rechargeThreshold)
          && (rechargeBlockMs == other.rechargeBlockMs);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return byteAllowance + rechargeBlockMs;
  }



}
