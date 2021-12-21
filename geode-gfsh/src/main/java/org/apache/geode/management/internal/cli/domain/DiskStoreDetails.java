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

package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.lang.MutableIdentifiable;
import org.apache.geode.internal.lang.ObjectUtils;

/**
 * The DiskStoreDetails class captures information about a particular disk store for a GemFire
 * distributed system member. Each disk store for a member should be captured in separate instance
 * of this class.
 * </p>
 *
 * @see org.apache.geode.cache.DiskStore
 * @see org.apache.geode.cache.DiskStoreFactory
 * @see org.apache.geode.lang.Identifiable
 * @see java.io.Serializable
 * @see java.lang.Comparable
 * @see java.lang.Iterable
 * @see java.util.UUID
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class DiskStoreDetails implements Comparable<DiskStoreDetails>, MutableIdentifiable<UUID>,
    Iterable<DiskStoreDetails.DiskDirDetails>, Serializable {

  public static final String DEFAULT_DISK_STORE_NAME = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;

  protected static final boolean DEFAULT_ALLOW_FORCE_COMPACTION = false;
  protected static final boolean DEFAULT_AUTO_COMPACT = true;

  protected static final int DEFAULT_COMPACTION_THRESHOLD = 50;
  protected static final int DEFAULT_QUEUE_SIZE = 0;
  protected static final int DEFAULT_WRITE_BUFFER_SIZE = 32768;

  protected static final long DEFAULT_MAX_OPLOG_SIZE = 1024l;
  protected static final long DEFAULT_TIME_INTERVALE = 1000l;

  private Boolean allowForceCompaction;
  private Boolean autoCompact;
  private Boolean offline;
  private Boolean pdxSerializationMetaDataStored;

  private Integer compactionThreshold;
  private Integer queueSize;
  private Integer writeBufferSize;

  private Float diskUsageWarningPercentage;
  private Float diskUsageCriticalPercentage;

  private final Set<AsyncEventQueueDetails> asyncEventQueueDetailsSet =
      new TreeSet<AsyncEventQueueDetails>();

  private final Set<CacheServerDetails> cacheServerDetailsSet = new TreeSet<CacheServerDetails>();

  private final Set<DiskDirDetails> diskDirDetailsSet = new TreeSet<DiskDirDetails>();

  private final Set<GatewayDetails> gatewayDetailsSet = new TreeSet<GatewayDetails>();

  private final Set<RegionDetails> regionDetailsSet = new TreeSet<RegionDetails>();

  private Long maxOplogSize;
  private Long timeInterval;

  private final String memberId;
  private String memberName;
  private final String name;

  private UUID id;

  protected static void assertNotNull(final Object obj, final String message,
      final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  // NOTE sort nulls last
  protected static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0
        : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  @SuppressWarnings("null")
  public DiskStoreDetails(final String name, final String memberId) {
    this(null, name, memberId, null);
  }

  @SuppressWarnings("null")
  public DiskStoreDetails(final UUID id, final String name, final String memberId) {
    this(id, name, memberId, null);
  }

  public DiskStoreDetails(final UUID id, final String name, final String memberId,
      final String memberName) {
    assertNotNull(name, "The name of the disk store cannot be null!");
    assertNotNull(memberId,
        "The id of the member to which the disk store (%1$s) belongs cannot be null!", name);
    this.id = id;
    this.name = name;
    this.memberId = memberId;
    this.memberName = memberName;
  }

  public Boolean getAllowForceCompaction() {
    return allowForceCompaction;
  }

  public boolean isAllowForceCompaction() {
    return Boolean.TRUE.equals(getAllowForceCompaction());
  }

  public void setAllowForceCompaction(final Boolean allowForceCompaction) {
    this.allowForceCompaction = allowForceCompaction;
  }

  public Boolean getAutoCompact() {
    return autoCompact;
  }

  public boolean isAutoCompact() {
    return Boolean.TRUE.equals(getAutoCompact());
  }

  public void setAutoCompact(final Boolean autoCompact) {
    this.autoCompact = autoCompact;
  }

  public Integer getCompactionThreshold() {
    return compactionThreshold;
  }

  public void setCompactionThreshold(final Integer compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public void setId(final UUID id) {
    this.id = id;
  }

  public Long getMaxOplogSize() {
    return maxOplogSize;
  }

  public void setMaxOplogSize(final Long maxOplogSize) {
    this.maxOplogSize = maxOplogSize;
  }

  public String getMemberId() {
    return memberId;
  }

  public String getMemberName() {
    return memberName;
  }

  public void setMemberName(final String memberName) {
    this.memberName = memberName;
  }

  public String getName() {
    return name;
  }

  public Boolean getOffline() {
    return offline;
  }

  public String getOfflineAsString(final String online, final String offline,
      final String nullValue) {
    return (getOffline() == null ? nullValue : (isOffline() ? offline : online));
  }

  public boolean isOffline() {
    return Boolean.TRUE.equals(getOffline());
  }

  public void setOffline(final Boolean offline) {
    this.offline = offline;
  }

  public Boolean getPdxSerializationMetaDataStored() {
    return pdxSerializationMetaDataStored;
  }

  public boolean isPdxSerializationMetaDataStored() {
    return Boolean.TRUE.equals(getPdxSerializationMetaDataStored());
  }

  public void setPdxSerializationMetaDataStored(final Boolean pdxSerializationMetaDataStored) {
    this.pdxSerializationMetaDataStored = pdxSerializationMetaDataStored;
  }

  public Integer getQueueSize() {
    return queueSize;
  }

  public void setQueueSize(final Integer queueSize) {
    this.queueSize = queueSize;
  }

  public Long getTimeInterval() {
    return timeInterval;
  }

  public void setTimeInterval(final Long timeInterval) {
    this.timeInterval = timeInterval;
  }

  public Integer getWriteBufferSize() {
    return writeBufferSize;
  }

  public void setWriteBufferSize(final Integer writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
  }

  public Float getDiskUsageWarningPercentage() {
    return diskUsageWarningPercentage;
  }

  public void setDiskUsageWarningPercentage(Float warningPct) {
    diskUsageWarningPercentage = warningPct;
  }

  public Float getDiskUsageCriticalPercentage() {
    return diskUsageCriticalPercentage;
  }

  public void setDiskUsageCriticalPercentage(Float criticalPct) {
    diskUsageCriticalPercentage = criticalPct;
  }

  public boolean add(final AsyncEventQueueDetails asyncEventQueueDetails) {
    assertNotNull(asyncEventQueueDetails,
        "Details concerning Asynchronous Event Queues that use this disk store ($1%s) cannot be null!",
        getName());
    return asyncEventQueueDetailsSet.add(asyncEventQueueDetails);
  }

  public boolean add(final CacheServerDetails cacheServerDetails) {
    assertNotNull(cacheServerDetails,
        "Details concerning Cache Servers that use this disk store (%1$s) cannot be null!",
        getName());
    return cacheServerDetailsSet.add(cacheServerDetails);
  }

  public boolean add(final DiskDirDetails diskDirDetails) {
    assertNotNull(diskDirDetails,
        "Details for the disk store's (%1$s) directory information cannot be null!", getName());
    return diskDirDetailsSet.add(diskDirDetails);
  }

  public boolean add(final GatewayDetails gatewayDetails) {
    assertNotNull(gatewayDetails,
        "Details concerning Gateways that use this disk store (%1$s) cannot be null!", getName());
    return gatewayDetailsSet.add(gatewayDetails);
  }

  public boolean add(final RegionDetails regionDetails) {
    assertNotNull(regionDetails,
        "Details concerning Regions that use this disk store (%1$%s) cannot be null!", getName());
    return regionDetailsSet.add(regionDetails);
  }

  @Override
  public int compareTo(final DiskStoreDetails diskStoreDetails) {
    int comparisonValue = compare(getMemberName(), diskStoreDetails.getMemberName());
    comparisonValue = (comparisonValue != 0 ? comparisonValue
        : compare(getMemberId(), diskStoreDetails.getMemberId()));
    return (comparisonValue != 0 ? comparisonValue
        : getName().compareTo(diskStoreDetails.getName()));
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof DiskStoreDetails)) {
      return false;
    }

    final DiskStoreDetails that = (DiskStoreDetails) obj;

    return ObjectUtils.equalsIgnoreNull(getId(), that.getId())
        && ObjectUtils.equals(getName(), that.getName())
        && ObjectUtils.equals(getMemberId(), that.getMemberId());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getId());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getName());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getMemberId());
    return hashValue;
  }

  @Override
  public Iterator<DiskDirDetails> iterator() {
    return Collections.unmodifiableSet(diskDirDetailsSet).iterator();
  }

  public Iterable<AsyncEventQueueDetails> iterateAsyncEventQueues() {
    return Collections.unmodifiableSet(asyncEventQueueDetailsSet);
  }

  public Iterable<CacheServerDetails> iterateCacheServers() {
    return Collections.unmodifiableSet(cacheServerDetailsSet);
  }

  public Iterable<GatewayDetails> iterateGateways() {
    return Collections.unmodifiableSet(gatewayDetailsSet);
  }

  public Iterable<RegionDetails> iterateRegions() {
    return Collections.unmodifiableSet(regionDetailsSet);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());

    buffer.append(" {id = ").append(getId());
    buffer.append(", allowForceCompaction = ").append(getAllowForceCompaction());
    buffer.append(", autoCompact = ").append(getAutoCompact());
    buffer.append(", compactionThreshold = ").append(getCompactionThreshold());
    buffer.append(", maxOplogSize = ").append(getMaxOplogSize());
    buffer.append(", memberId = ").append(getMemberId());
    buffer.append(", memberName = ").append(getMemberName());
    buffer.append(", name = ").append(getName());
    buffer.append(", offline = ").append(getOffline());
    buffer.append(", pdxSerializationMetaDataStored = ")
        .append(getPdxSerializationMetaDataStored());
    buffer.append(", queueSize = ").append(getQueueSize());
    buffer.append(", timeInterval = ").append(getTimeInterval());
    buffer.append(", writeBufferSize = ").append(getWriteBufferSize());
    buffer.append(", diskUsageWarningPercentage = ").append(getDiskUsageWarningPercentage());
    buffer.append(", diskUsageCriticalPercentage = ").append(getDiskUsageCriticalPercentage());
    buffer.append(", diskDirs = ").append(toString(diskDirDetailsSet));
    buffer.append(", asyncEventQueus = ").append(toString(asyncEventQueueDetailsSet));
    buffer.append(", cacheServers = ").append(toString(cacheServerDetailsSet));
    buffer.append(", gateways = ").append(toString(gatewayDetailsSet));
    buffer.append(", regions = ").append(toString(regionDetailsSet));
    buffer.append("}");

    return buffer.toString();
  }

  protected String toString(final Collection<?> collection) {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    for (final Object element : collection) {
      buffer.append(count++ > 0 ? ", " : "");
      buffer.append(element);
    }

    buffer.append("]");

    return buffer.toString();
  }

  public static class AsyncEventQueueDetails
      implements Comparable<AsyncEventQueueDetails>, Serializable {

    private final String id;

    public AsyncEventQueueDetails(final String id) {
      assertNotNull(id, "The id of the Asynchronous Event Queue cannot be null!");
      this.id = id;
    }

    public String getId() {
      return id;
    }

    @Override
    public int compareTo(final AsyncEventQueueDetails asyncEventQueueDetails) {
      return getId().compareTo(asyncEventQueueDetails.getId());
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof AsyncEventQueueDetails)) {
        return false;
      }

      final AsyncEventQueueDetails that = (AsyncEventQueueDetails) obj;

      return ObjectUtils.equals(getId(), that.getId());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getId());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {id =").append(getId());
      buffer.append("}");
      return buffer.toString();
    }
  }

  public static class CacheServerDetails implements Comparable<CacheServerDetails>, Serializable {

    private final int port;

    private final String bindAddress;
    private String hostName;

    public CacheServerDetails(final String bindAddress, final int port) {
      this.bindAddress = StringUtils.defaultIfBlank(bindAddress, "*");
      this.port = port;
    }

    public String getBindAddress() {
      return bindAddress;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(final String hostName) {
      this.hostName = hostName;
    }

    public int getPort() {
      return port;
    }

    @Override
    public int compareTo(final CacheServerDetails cacheServerDetails) {
      final int valueOfBindAddressComparison =
          getBindAddress().compareTo(cacheServerDetails.getBindAddress());
      return (valueOfBindAddressComparison != 0 ? valueOfBindAddressComparison
          : (getPort() - cacheServerDetails.getPort()));
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CacheServerDetails)) {
        return false;
      }

      final CacheServerDetails that = (CacheServerDetails) obj;

      return ObjectUtils.equals(getBindAddress(), that.getBindAddress())
          && ObjectUtils.equals(getPort(), that.getPort());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getBindAddress());
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getPort());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {bindAddress = ").append(getBindAddress());
      buffer.append(", hostName = ").append(getHostName());
      buffer.append(", port = ").append(getPort());
      buffer.append("}");
      return buffer.toString();
    }
  }

  public static class DiskDirDetails implements Comparable<DiskDirDetails>, Serializable {

    private final String absolutePath;

    private final int size;

    public DiskDirDetails(final String absolutePath) {
      this(absolutePath, 0);
    }

    public DiskDirDetails(final String absolutePath, final int size) {
      DiskStoreDetails.assertNotNull(absolutePath,
          "The directory location of the disk store cannot be null!");
      this.absolutePath = absolutePath;
      this.size = size;
    }

    public String getAbsolutePath() {
      return absolutePath;
    }

    public int getSize() {
      return size;
    }

    @Override
    public int compareTo(final DiskDirDetails diskDirDetails) {
      return getAbsolutePath().compareTo(diskDirDetails.getAbsolutePath());
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof DiskDirDetails)) {
        return false;
      }

      final DiskDirDetails that = (DiskDirDetails) obj;

      return ObjectUtils.equals(getAbsolutePath(), that.getAbsolutePath());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getAbsolutePath());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {absolutePath = ").append(getAbsolutePath());
      buffer.append(", size = ").append(getSize());
      buffer.append("}");
      return buffer.toString();
    }
  }

  public static class GatewayDetails implements Comparable<GatewayDetails>, Serializable {

    private boolean persistent;

    private final String id;

    public GatewayDetails(final String id) {
      assertNotNull(id, "The ID of the Gateway cannot be null!");
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public boolean isOverflowToDisk() {
      return true;
    }

    public boolean isPersistent() {
      return persistent;
    }

    public void setPersistent(final boolean persistent) {
      this.persistent = persistent;
    }

    @Override
    public int compareTo(final GatewayDetails gatewayDetails) {
      return getId().compareTo(gatewayDetails.getId());
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof GatewayDetails)) {
        return false;
      }

      final GatewayDetails that = (GatewayDetails) obj;

      return ObjectUtils.equals(getId(), that.getId());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getId());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {id = ").append(getId());
      buffer.append(", overflowToDisk = ").append(isOverflowToDisk());
      buffer.append(", persistent = ").append(isPersistent());
      buffer.append("}");
      return buffer.toString();
    }
  }

  public static class RegionDetails implements Comparable<RegionDetails>, Serializable {

    private boolean overflowToDisk;
    private boolean persistent;

    private final String fullPath;
    private final String name;

    public RegionDetails(final String fullPath, final String name) {
      assertNotNull(fullPath, "The full path of the Region in the Cache cannot be null!");
      assertNotNull(name, "The name of the Region @ (%1$s) cannot be null!", fullPath);
      this.name = name;
      this.fullPath = fullPath;
    }

    public String getFullPath() {
      return fullPath;
    }

    public String getName() {
      return name;
    }

    public boolean isOverflowToDisk() {
      return overflowToDisk;
    }

    public void setOverflowToDisk(final boolean overflowToDisk) {
      this.overflowToDisk = overflowToDisk;
    }

    public boolean isPersistent() {
      return persistent;
    }

    public void setPersistent(final boolean persistent) {
      this.persistent = persistent;
    }

    @Override
    public int compareTo(final RegionDetails regionDetails) {
      return getFullPath().compareTo(regionDetails.getFullPath());
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof RegionDetails)) {
        return false;
      }

      final RegionDetails that = (RegionDetails) obj;

      return ObjectUtils.equals(getFullPath(), that.getFullPath());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.hashCode(getFullPath());
      return hashValue;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
      buffer.append(" {fullPath = ").append(getFullPath());
      buffer.append(", name = ").append(getName());
      buffer.append(", overflowToDisk = ").append(isOverflowToDisk());
      buffer.append(", persistent = ").append(isPersistent());
      buffer.append("}");
      return buffer.toString();
    }
  }

}
