/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.management.configuration;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.DiskStoreInfo;

@Experimental
public class DiskStore extends GroupableConfiguration<DiskStoreInfo> {
  public static final String DISK_STORE_CONFIG_ENDPOINT = "/diskstores";

  private String name;
  private Integer compactionThreshold;
  private Float diskUsageCriticalPercentage;
  private Float diskUsageWarningPercentage;
  private Long maxOplogSizeInBytes;
  private Integer queueSize;
  private Long timeInterval;
  private Integer writeBufferSize;
  private List<DiskDir> directories;
  private Boolean allowForceCompaction;
  private Boolean autoCompact;

  public Boolean isAutoCompact() {
    return autoCompact;
  }

  public void setAutoCompact(Boolean autoCompact) {
    this.autoCompact = autoCompact;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<DiskDir> getDirectories() {
    return directories;
  }

  public void setDirectories(List<DiskDir> directories) {
    this.directories = directories;
  }

  @JsonIgnore
  @Override
  public String getId() {
    return name;
  }

  public Integer getCompactionThreshold() {
    return compactionThreshold;
  }

  public void setCompactionThreshold(Integer compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
  }

  public Float getDiskUsageCriticalPercentage() {
    return diskUsageCriticalPercentage;
  }

  public void setDiskUsageCriticalPercentage(Float diskUsageCriticalPercentage) {
    this.diskUsageCriticalPercentage = diskUsageCriticalPercentage;
  }

  public Float getDiskUsageWarningPercentage() {
    return diskUsageWarningPercentage;
  }

  public void setDiskUsageWarningPercentage(Float diskUsageWarningPercentage) {
    this.diskUsageWarningPercentage = diskUsageWarningPercentage;
  }

  public Long getMaxOplogSizeInBytes() {
    return maxOplogSizeInBytes;
  }

  public void setMaxOplogSizeInBytes(Long maxOplogSize) {
    maxOplogSizeInBytes = maxOplogSize;
  }

  public Integer getQueueSize() {
    return queueSize;
  }

  public void setQueueSize(Integer queueSize) {
    this.queueSize = queueSize;
  }

  public Long getTimeInterval() {
    return timeInterval;
  }

  public void setTimeInterval(Long timeInterval) {
    this.timeInterval = timeInterval;
  }

  public Integer getWriteBufferSize() {
    return writeBufferSize;
  }

  public void setWriteBufferSize(Integer writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
  }

  public Boolean isAllowForceCompaction() {
    return allowForceCompaction;
  }

  public void setAllowForceCompaction(Boolean allowForceCompaction) {
    this.allowForceCompaction = allowForceCompaction;
  }

  @Override
  public Links getLinks() {
    return new Links(getId(), DISK_STORE_CONFIG_ENDPOINT);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DiskStore diskStore = (DiskStore) o;
    return Objects.equals(name, diskStore.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }
}
