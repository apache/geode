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

package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * Used to carry arguments between gfsh region command implementations and the functions that do the
 * work for those commands.
 *
 * @since GemFire 7.0
 */
public class RegionFunctionArgs implements Serializable {
  private static final long serialVersionUID = 2204943186081037301L;

  private String regionPath;
  private RegionShortcut regionShortcut;
  private String templateRegion;
  private boolean skipIfExists;
  private String keyConstraint;
  private String valueConstraint;
  private Boolean statisticsEnabled;
  private RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime;
  private RegionFunctionArgs.ExpirationAttrs entryExpirationTTL;
  private RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime;
  private RegionFunctionArgs.ExpirationAttrs regionExpirationTTL;
  private String diskStore;
  private Boolean diskSynchronous;
  private Boolean enableAsyncConflation;
  private Boolean enableSubscriptionConflation;
  private Set<String> cacheListeners = Collections.emptySet();
  private String cacheLoader;
  private String cacheWriter;
  private Set<String> asyncEventQueueIds = Collections.emptySet();
  private Set<String> gatewaySenderIds = Collections.emptySet();
  private Boolean concurrencyChecksEnabled;
  private Boolean cloningEnabled;
  private Boolean mcastEnabled;
  private Integer concurrencyLevel;
  private PartitionArgs partitionArgs;
  private Integer evictionMax;
  private String compressor;
  private Boolean offHeap;
  private RegionAttributes<?, ?> regionAttributes;

  public RegionFunctionArgs() {
    this.partitionArgs = new PartitionArgs();
  }

  public void setRegionPath(String regionPath) {
    this.regionPath = regionPath;
  }

  public void setTemplateRegion(String templateRegion) {
    this.templateRegion = templateRegion;
  }

  public void setRegionShortcut(RegionShortcut regionShortcut) {
    this.regionShortcut = regionShortcut;
  }


  public void setSkipIfExists(boolean skipIfExists) {
    this.skipIfExists = skipIfExists;
  }

  public void setKeyConstraint(String keyConstraint) {
    this.keyConstraint = keyConstraint;
  }

  public void setValueConstraint(String valueConstraint) {
    this.valueConstraint = valueConstraint;
  }

  public void setStatisticsEnabled(Boolean statisticsEnabled) {
    this.statisticsEnabled = statisticsEnabled;
  }

  public void setEntryExpirationIdleTime(Integer timeout, String action) {
    if (timeout != null) {
      this.entryExpirationIdleTime = new ExpirationAttrs(
          RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_IDLE, timeout, action);
    }
  }

  public void setEntryExpirationTTL(Integer timeout, String action) {
    if (timeout != null) {
      this.entryExpirationTTL = new ExpirationAttrs(
          RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_TTL, timeout, action);
    }
  }

  public void setRegionExpirationIdleTime(Integer timeout, String action) {
    if (timeout != null) {
      this.regionExpirationIdleTime = new ExpirationAttrs(
          RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_IDLE, timeout, action);
    }
  }

  public void setRegionExpirationTTL(Integer timeout, String action) {
    if (timeout != null) {
      this.regionExpirationTTL = new ExpirationAttrs(
          RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_TTL, timeout, action);
    }
  }

  public void setDiskStore(String diskStore) {
    this.diskStore = diskStore;
  }

  public void setDiskSynchronous(Boolean diskSynchronous) {
    this.diskSynchronous = diskSynchronous;
  }

  public void setEnableAsyncConflation(Boolean enableAsyncConflation) {
    this.enableAsyncConflation = enableAsyncConflation;
  }

  public void setEnableSubscriptionConflation(Boolean enableSubscriptionConflation) {
    this.enableSubscriptionConflation = enableSubscriptionConflation;
  }

  public void setCacheListeners(String[] cacheListeners) {
    if (cacheListeners != null) {
      this.cacheListeners = Arrays.stream(cacheListeners).collect(Collectors.toSet());
    }
  }

  public void setCacheLoader(String cacheLoader) {
    this.cacheLoader = cacheLoader;
  }

  public void setCacheWriter(String cacheWriter) {
    this.cacheWriter = cacheWriter;
  }

  public void setAsyncEventQueueIds(String[] asyncEventQueueIds) {
    if (asyncEventQueueIds != null) {
      this.asyncEventQueueIds = Arrays.stream(asyncEventQueueIds).collect(Collectors.toSet());
    }
  }

  public void setGatewaySenderIds(String[] gatewaySenderIds) {
    if (gatewaySenderIds != null) {
      this.gatewaySenderIds = Arrays.stream(gatewaySenderIds).collect(Collectors.toSet());
    }
  }

  public void setConcurrencyChecksEnabled(Boolean concurrencyChecksEnabled) {
    this.concurrencyChecksEnabled = concurrencyChecksEnabled;
  }

  public void setCloningEnabled(Boolean cloningEnabled) {
    this.cloningEnabled = cloningEnabled;
  }

  public void setMcastEnabled(Boolean mcastEnabled) {
    this.mcastEnabled = mcastEnabled;
  }

  public void setConcurrencyLevel(Integer concurrencyLevel) {
    this.concurrencyLevel = concurrencyLevel;
  }

  public void setPartitionArgs(String prColocatedWith, Integer prLocalMaxMemory,
      Long prRecoveryDelay, Integer prRedundantCopies, Long prStartupRecoveryDelay,
      Long prTotalMaxMemory, Integer prTotalNumBuckets, String partitionResolver) {
    partitionArgs.setPrColocatedWith(prColocatedWith);
    partitionArgs.setPrLocalMaxMemory(prLocalMaxMemory);
    partitionArgs.setPrRecoveryDelay(prRecoveryDelay);
    partitionArgs.setPrRedundantCopies(prRedundantCopies);
    partitionArgs.setPrStartupRecoveryDelay(prStartupRecoveryDelay);
    partitionArgs.setPrTotalMaxMemory(prTotalMaxMemory);
    partitionArgs.setPrTotalNumBuckets(prTotalNumBuckets);
    partitionArgs.setPartitionResolver(partitionResolver);
  }

  public void setEvictionMax(Integer evictionMax) {
    this.evictionMax = evictionMax;
  }

  public void setCompressor(String compressor) {
    this.compressor = compressor;
  }

  public void setOffHeap(Boolean offHeap) {
    this.offHeap = offHeap;
  }

  public void setRegionAttributes(RegionAttributes<?, ?> regionAttributes) {
    this.regionAttributes = regionAttributes;
  }

  /**
   * @return the regionPath
   */
  public String getRegionPath() {
    return this.regionPath;
  }

  /**
   * @return the regionShortcut
   */
  public RegionShortcut getRegionShortcut() {
    return this.regionShortcut;
  }

  /**
   * @return the templateRegion
   */
  public String getTemplateRegion() {
    return this.templateRegion;
  }

  /**
   * @return the skipIfExists
   */
  public Boolean isSkipIfExists() {
    return this.skipIfExists;
  }

  /**
   * @return the keyConstraint
   */
  public String getKeyConstraint() {
    return this.keyConstraint;
  }

  /**
   * @return the valueConstraint
   */
  public String getValueConstraint() {
    return this.valueConstraint;
  }

  /**
   * @return the statisticsEnabled
   */
  public Boolean isStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  /**
   * @return the entryExpirationIdleTime
   */
  public RegionFunctionArgs.ExpirationAttrs getEntryExpirationIdleTime() {
    return this.entryExpirationIdleTime;
  }

  /**
   * @return the entryExpirationTTL
   */
  public RegionFunctionArgs.ExpirationAttrs getEntryExpirationTTL() {
    return this.entryExpirationTTL;
  }

  /**
   * @return the regionExpirationIdleTime
   */
  public RegionFunctionArgs.ExpirationAttrs getRegionExpirationIdleTime() {
    return this.regionExpirationIdleTime;
  }

  /**
   * @return the regionExpirationTTL
   */
  public RegionFunctionArgs.ExpirationAttrs getRegionExpirationTTL() {
    return this.regionExpirationTTL;
  }

  /**
   * @return the diskStore
   */
  public String getDiskStore() {
    return this.diskStore;
  }

  /**
   * @return the diskSynchronous
   */
  public Boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }

  public Boolean isOffHeap() {
    return this.offHeap;
  }

  /**
   * @return the enableAsyncConflation
   */
  public Boolean isEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  /**
   * @return the enableSubscriptionConflation
   */
  public Boolean isEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  /**
   * @return the cacheListeners
   */
  public Set<String> getCacheListeners() {
    if (this.cacheListeners == null) {
      return null;
    }
    return Collections.unmodifiableSet(this.cacheListeners);
  }

  /**
   * @return the cacheLoader
   */
  public String getCacheLoader() {
    return this.cacheLoader;
  }

  /**
   * @return the cacheWriter
   */
  public String getCacheWriter() {
    return this.cacheWriter;
  }

  /**
   * @return the asyncEventQueueIds
   */
  public Set<String> getAsyncEventQueueIds() {
    if (this.asyncEventQueueIds == null) {
      return null;
    }
    return Collections.unmodifiableSet(this.asyncEventQueueIds);
  }

  /**
   * @return the gatewaySenderIds
   */
  public Set<String> getGatewaySenderIds() {
    if (this.gatewaySenderIds == null) {
      return null;
    }
    return Collections.unmodifiableSet(this.gatewaySenderIds);
  }

  /**
   * @return the concurrencyChecksEnabled
   */
  public Boolean isConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  /**
   * @return the cloningEnabled
   */
  public Boolean isCloningEnabled() {
    return this.cloningEnabled;
  }

  /**
   * @return the mcastEnabled setting
   */
  public Boolean isMcastEnabled() {
    return this.mcastEnabled;
  }

  /**
   * @return the concurrencyLevel
   */
  public Integer getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  public boolean withPartitioning() {
    return hasPartitionAttributes()
        || (this.regionShortcut != null && this.regionShortcut.name().startsWith("PARTITION"));
  }

  /**
   * @return the partitionArgs
   */
  public boolean hasPartitionAttributes() {
    return this.partitionArgs.hasPartitionAttributes();
  }

  /**
   * @return the partitionArgs
   */
  public PartitionArgs getPartitionArgs() {
    return this.partitionArgs;
  }

  /**
   * @return the evictionMax
   */
  public Integer getEvictionMax() {
    return this.evictionMax;
  }

  /**
   * @return the compressor.
   */
  public String getCompressor() {
    return this.compressor;
  }

  /**
   * @return the regionAttributes
   */
  @SuppressWarnings("unchecked")
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    return (RegionAttributes<K, V>) this.regionAttributes;
  }

  public static class ExpirationAttrs implements Serializable {
    private static final long serialVersionUID = 1474255033398008062L;

    private final ExpirationFor type;
    private final ExpirationAttributes timeAndAction;

    public ExpirationAttrs(ExpirationFor type, Integer time, String action) {
      this.type = type;
      if (time != null) {
        this.timeAndAction = new ExpirationAttributes(time, getExpirationAction(action));
      } else {
        this.timeAndAction = new ExpirationAttributes(0, getExpirationAction(action));
      }
    }

    public ExpirationAttributes convertToExpirationAttributes() {
      return timeAndAction;
    }

    /**
     * @return the type
     */
    public ExpirationFor getType() {
      return type;
    }

    /**
     * @return the time
     */
    public Integer getTime() {
      return timeAndAction.getTimeout();
    }

    /**
     * @return the action
     */
    public ExpirationAction getAction() {
      return timeAndAction.getAction();
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(ExpirationAttrs.class.getSimpleName() + " [type=");
      builder.append(type);
      builder.append(", time=");
      builder.append(timeAndAction.getTimeout());
      builder.append(", action=");
      builder.append(timeAndAction.getAction());
      builder.append("]");
      return builder.toString();
    }

    private static ExpirationAction getExpirationAction(String action) {
      if (action == null) {
        return ExpirationAttributes.DEFAULT.getAction();
      }
      action = action.replace('-', '_');
      if (action.equalsIgnoreCase(ExpirationAction.DESTROY.toString())) {
        return ExpirationAction.DESTROY;
      } else if (action.equalsIgnoreCase(ExpirationAction.INVALIDATE.toString())) {
        return ExpirationAction.INVALIDATE;
      } else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_DESTROY.toString())) {
        return ExpirationAction.LOCAL_DESTROY;
      } else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_INVALIDATE.toString())) {
        return ExpirationAction.LOCAL_INVALIDATE;
      } else {
        throw new IllegalArgumentException(
            CliStrings.format(CliStrings.CREATE_REGION__MSG__EXPIRATION_ACTION_0_IS_NOT_VALID,
                new Object[] {action}));
      }
    }

    public enum ExpirationFor {
      REGION_IDLE, REGION_TTL, ENTRY_IDLE, ENTRY_TTL
    }
  }

  public static class PartitionArgs implements Serializable {
    private static final long serialVersionUID = 5907052187323280919L;

    private String prColocatedWith;
    private Integer prLocalMaxMemory;
    private Long prRecoveryDelay;
    private Integer prRedundantCopies;
    private Long prStartupRecoveryDelay;
    private Long prTotalMaxMemory;
    private Integer prTotalNumBuckets;
    private String partitionResolver;

    public PartitionArgs() {}

    /**
     * @return the hasPartitionAttributes
     */
    public Boolean hasPartitionAttributes() {
      return !getUserSpecifiedPartitionAttributes().isEmpty();
    }

    /**
     * @return the userSpecifiedPartitionAttributes
     */
    public Set<String> getUserSpecifiedPartitionAttributes() {
      Set<String> userSpecifiedPartitionAttributes = new HashSet<>();

      if (this.prColocatedWith != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__COLOCATEDWITH);
      }
      if (this.prLocalMaxMemory != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__LOCALMAXMEMORY);
      }
      if (this.prRecoveryDelay != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__RECOVERYDELAY);
      }
      if (this.prRedundantCopies != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__REDUNDANTCOPIES);
      }
      if (this.prStartupRecoveryDelay != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY);
      }
      if (this.prTotalMaxMemory != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__TOTALMAXMEMORY);
      }
      if (this.prTotalNumBuckets != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__TOTALNUMBUCKETS);
      }
      if (this.partitionResolver != null) {
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__PARTITION_RESOLVER);
      }

      return userSpecifiedPartitionAttributes;
    }

    public void setPrColocatedWith(String prColocatedWith) {
      if (prColocatedWith != null) {
        this.prColocatedWith = prColocatedWith;
      }
    }

    public void setPrLocalMaxMemory(Integer prLocalMaxMemory) {
      if (prLocalMaxMemory != null) {
        this.prLocalMaxMemory = prLocalMaxMemory;
      }
    }

    public void setPrRecoveryDelay(Long prRecoveryDelay) {
      if (prRecoveryDelay != null) {
        this.prRecoveryDelay = prRecoveryDelay;
      }
    }

    public void setPrRedundantCopies(Integer prRedundantCopies) {
      if (prRedundantCopies != null) {
        this.prRedundantCopies = prRedundantCopies;
      }
    }

    public void setPrStartupRecoveryDelay(Long prStartupRecoveryDelay) {
      if (prStartupRecoveryDelay != null) {
        this.prStartupRecoveryDelay = prStartupRecoveryDelay;
      }
    }

    public void setPrTotalMaxMemory(Long prTotalMaxMemory) {
      if (prTotalMaxMemory != null) {
        this.prTotalMaxMemory = prTotalMaxMemory;
      }
    }

    public void setPrTotalNumBuckets(Integer prTotalNumBuckets) {
      if (prTotalNumBuckets != null) {
        this.prTotalNumBuckets = prTotalNumBuckets;
      }
    }

    public void setPartitionResolver(String partitionResolver) {
      if (partitionResolver != null) {
        this.partitionResolver = partitionResolver;
      }
    }

    /**
     * @return the prColocatedWith
     */
    public String getPrColocatedWith() {
      return prColocatedWith;
    }

    /**
     * @return the prLocalMaxMemory
     */
    public Integer getPrLocalMaxMemory() {
      return prLocalMaxMemory;
    }

    /**
     * @return the prRecoveryDelay
     */
    public Long getPrRecoveryDelay() {
      return prRecoveryDelay;
    }

    /**
     * @return the prRedundantCopies
     */
    public Integer getPrRedundantCopies() {
      return prRedundantCopies;
    }

    /**
     * @return the prStartupRecoveryDelay
     */
    public Long getPrStartupRecoveryDelay() {
      return prStartupRecoveryDelay;
    }

    /**
     * @return the prTotalMaxMemory
     */
    public Long getPrTotalMaxMemory() {
      return prTotalMaxMemory;
    }

    /**
     * @return the prTotalNumBuckets
     */
    public Integer getPrTotalNumBuckets() {
      return prTotalNumBuckets;
    }

    /**
     * @return the partition resolver
     */
    public String getPartitionResolver() {
      return partitionResolver;
    }
  }
}
