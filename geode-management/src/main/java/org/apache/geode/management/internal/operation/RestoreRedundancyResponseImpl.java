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

package org.apache.geode.management.internal.operation;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.runtime.RegionRedundancyStatusSerializable;
import org.apache.geode.management.runtime.RestoreRedundancyResponse;

public class RestoreRedundancyResponseImpl
    implements RestoreRedundancyResponse {

  private boolean success;
  private int totalPrimaryTransfersCompleted = 0;
  private long totalPrimaryTransferTime = 0;
  private String statusMessage;
  private final List<RegionRedundancyStatusSerializable> zeroRedundancyRegionResults =
      new ArrayList<>();
  private final List<RegionRedundancyStatusSerializable> underRedundancyRegionResults =
      new ArrayList<>();
  private final List<RegionRedundancyStatusSerializable> satisfiedRedundancyRegionResults =
      new ArrayList<>();

  public RestoreRedundancyResponseImpl() {
    success = false;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public void setStatusMessage(String statusMessage) {
    // capitalize and punctuate properly since this can be accessed directly
    this.statusMessage = new ClusterManagementResult(null, statusMessage).getStatusMessage();
  }

  @Override
  public boolean getSuccess() {
    return success;
  }

  @Override
  public String getStatusMessage() {
    return statusMessage;
  }

  public List<RegionRedundancyStatusSerializable> getZeroRedundancyRegionResults() {
    return zeroRedundancyRegionResults;
  }

  public List<RegionRedundancyStatusSerializable> getUnderRedundancyRegionResults() {
    return underRedundancyRegionResults;
  }

  public List<RegionRedundancyStatusSerializable> getSatisfiedRedundancyRegionResults() {
    return satisfiedRedundancyRegionResults;
  }

  public void setZeroRedundancyRegionResults(
      List<RegionRedundancyStatusSerializable> zeroRedundancyRegionResults) {
    this.zeroRedundancyRegionResults.addAll(zeroRedundancyRegionResults);
  }

  public void setUnderRedundancyRegionResults(
      List<RegionRedundancyStatusSerializable> underRedundancyRegionResults) {
    this.underRedundancyRegionResults.addAll(underRedundancyRegionResults);
  }

  public void setSatisfiedRedundancyRegionResults(
      List<RegionRedundancyStatusSerializable> satisfiedRedundancyRegionResults) {
    this.satisfiedRedundancyRegionResults.addAll(satisfiedRedundancyRegionResults);
  }


  public int getTotalPrimaryTransfersCompleted() {
    return totalPrimaryTransfersCompleted;
  }

  public long getTotalPrimaryTransferTime() {
    return totalPrimaryTransferTime;
  }

  public void setTotalPrimaryTransfersCompleted(int totalPrimaryTransfersCompleted) {
    this.totalPrimaryTransfersCompleted = totalPrimaryTransfersCompleted;
  }

  public void setTotalPrimaryTransferTime(long totalPrimaryTransferTime) {
    this.totalPrimaryTransferTime = totalPrimaryTransferTime;
  }

  @Override
  public String toString() {
    return "RestoreRedundancyResponseImpl{" +
        ", success=" + success +
        ", statusMessage='" + statusMessage + '\'' +
        ", zeroRedundancyRegionResults=" + zeroRedundancyRegionResults +
        ", underRedundancyRegionResults=" + underRedundancyRegionResults +
        ", satisfiedRedundancyRegionResults=" + satisfiedRedundancyRegionResults +
        '}';
  }
}
