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

  private  String regionName;
  private  int configuredRedundancy;
  private  int actualRedundancy;
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
