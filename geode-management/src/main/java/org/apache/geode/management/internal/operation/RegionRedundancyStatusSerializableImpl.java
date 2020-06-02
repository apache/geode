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

import org.apache.geode.management.runtime.RegionRedundancyStatusSerializable;

public class RegionRedundancyStatusSerializableImpl implements RegionRedundancyStatusSerializable {

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public void setConfiguredRedundancy(int configuredRedundancy) {
    this.configuredRedundancy = configuredRedundancy;
  }

  public void setActualRedundancy(int actualRedundancy) {
    this.actualRedundancy = actualRedundancy;
  }

  public void setStatus(RedundancyStatus status) {
    this.status = status;
  }

  private String regionName;
  private int configuredRedundancy;
  private int actualRedundancy;
  private RedundancyStatus status;

  public RegionRedundancyStatusSerializableImpl() {}

  @Override
  public String getRegionName() {
    return regionName;
  }

  @Override
  public int getConfiguredRedundancy() {
    return configuredRedundancy;
  }

  @Override
  public int getActualRedundancy() {
    return actualRedundancy;
  }

  @Override
  public RedundancyStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "RegionRedundancyStatusSerializableImpl{" +
        "regionName='" + regionName + '\'' +
        ", configuredRedundancy=" + configuredRedundancy +
        ", actualRedundancy=" + actualRedundancy +
        ", status=" + status +
        '}';
  }
}
