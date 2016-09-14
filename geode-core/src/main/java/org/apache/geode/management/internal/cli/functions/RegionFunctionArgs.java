/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Used to carry arguments between gfsh region command implementations and the functions
 * that do the work for those commands.
 * 
 * @since GemFire 7.0
 */
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class RegionFunctionArgs implements Serializable {
  private static final long serialVersionUID = -5158224572470173267L;
  
  private final String regionPath;
  private final RegionShortcut regionShortcut; 
  private final String useAttributesFrom;
  private final Boolean skipIfExists; 
  private final String keyConstraint;
  private final String valueConstraint;
  private Boolean statisticsEnabled;
  private final boolean isSetStatisticsEnabled;
  private final RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime;
  private final RegionFunctionArgs.ExpirationAttrs entryExpirationTTL;
  private final RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime;
  private final RegionFunctionArgs.ExpirationAttrs regionExpirationTTL;      
  private final String diskStore;
  private Boolean diskSynchronous;
  private final boolean isSetDiskSynchronous;
  private Boolean enableAsyncConflation;
  private final boolean isSetEnableAsyncConflation;
  private Boolean enableSubscriptionConflation;
  private final boolean isSetEnableSubscriptionConflation;
  private final Set<String> cacheListeners;
  private final String cacheLoader;
  private final String cacheWriter;
  private final Set<String> asyncEventQueueIds;
  private final Set<String> gatewaySenderIds;
  private Boolean concurrencyChecksEnabled;
  private final boolean isSetConcurrencyChecksEnabled;
  private Boolean cloningEnabled;
  private final boolean isSetCloningEnabled;
  private Boolean mcastEnabled;
  private final boolean isSetMcastEnabled;
  private Integer concurrencyLevel;
  private final boolean isSetConcurrencyLevel;
  private final PartitionArgs partitionArgs;
  private final Integer evictionMax;
  private String compressor;
  private final boolean isSetCompressor;
  private Boolean offHeap;
  private final boolean isSetOffHeap;
  private RegionAttributes<?, ?> regionAttributes;

  public RegionFunctionArgs(String regionPath,
      RegionShortcut regionShortcut, String useAttributesFrom,
      boolean skipIfExists, String keyConstraint, String valueConstraint,
      Boolean statisticsEnabled, 
      RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime, 
      RegionFunctionArgs.ExpirationAttrs entryExpirationTTL, 
      RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime, 
      RegionFunctionArgs.ExpirationAttrs regionExpirationTTL, String diskStore,
      Boolean diskSynchronous, Boolean enableAsyncConflation,
      Boolean enableSubscriptionConflation, String[] cacheListeners,
      String cacheLoader, String cacheWriter, String[] asyncEventQueueIds,
      String[] gatewaySenderIds, Boolean concurrencyChecksEnabled,
      Boolean cloningEnabled, Integer concurrencyLevel, String prColocatedWith,
      Integer prLocalMaxMemory, Long prRecoveryDelay,
      Integer prRedundantCopies, Long prStartupRecoveryDelay,
      Long prTotalMaxMemory, Integer prTotalNumBuckets, Integer evictionMax,
      String compressor, Boolean offHeap, Boolean mcastEnabled) {
    this.regionPath = regionPath;
    this.regionShortcut = regionShortcut;
    this.useAttributesFrom = useAttributesFrom;
    this.skipIfExists = skipIfExists;
    this.keyConstraint = keyConstraint;
    this.valueConstraint = valueConstraint;
    this.evictionMax = evictionMax;
    this.isSetStatisticsEnabled = statisticsEnabled != null;
    if (this.isSetStatisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
    }
    this.entryExpirationIdleTime = entryExpirationIdleTime;
    this.entryExpirationTTL = entryExpirationTTL;
    this.regionExpirationIdleTime = regionExpirationIdleTime;
    this.regionExpirationTTL = regionExpirationTTL;
    this.diskStore = diskStore;
    this.isSetDiskSynchronous = diskSynchronous != null;
    if (this.isSetDiskSynchronous) {
      this.diskSynchronous = diskSynchronous;
    }
    this.isSetEnableAsyncConflation = enableAsyncConflation != null;
    if (this.isSetEnableAsyncConflation) {
      this.enableAsyncConflation = enableAsyncConflation;
    }
    this.isSetEnableSubscriptionConflation = enableSubscriptionConflation != null;
    if (this.isSetEnableSubscriptionConflation) {
      this.enableSubscriptionConflation = enableSubscriptionConflation;
    }
    if (cacheListeners != null) {
      this.cacheListeners = new LinkedHashSet<String>();
      this.cacheListeners.addAll(Arrays.asList(cacheListeners));
    } else {
      this.cacheListeners = null;
    }
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    if (asyncEventQueueIds != null) {
      this.asyncEventQueueIds = new LinkedHashSet<String>();
      this.asyncEventQueueIds.addAll(Arrays.asList(asyncEventQueueIds));
    } else {
      this.asyncEventQueueIds = null;
    }
    if (gatewaySenderIds != null) {
      this.gatewaySenderIds = new LinkedHashSet<String>();
      this.gatewaySenderIds.addAll(Arrays.asList(gatewaySenderIds));
    } else {
      this.gatewaySenderIds = null;
    }
    this.isSetConcurrencyChecksEnabled = concurrencyChecksEnabled != null;
    if (this.isSetConcurrencyChecksEnabled) {
      this.concurrencyChecksEnabled = concurrencyChecksEnabled;
    }
    this.isSetCloningEnabled = cloningEnabled != null;
    if (this.isSetCloningEnabled) {
      this.cloningEnabled = cloningEnabled;
    }
    this.isSetMcastEnabled = mcastEnabled != null;
    if (isSetMcastEnabled) {
      this.mcastEnabled = mcastEnabled;
    }
    this.isSetConcurrencyLevel = concurrencyLevel != null;
    if (this.isSetConcurrencyLevel) {
      this.concurrencyLevel = concurrencyLevel;
    }
    this.partitionArgs = new PartitionArgs(prColocatedWith,
        prLocalMaxMemory, prRecoveryDelay, prRedundantCopies,
        prStartupRecoveryDelay, prTotalMaxMemory, prTotalNumBuckets);
    
    this.isSetCompressor = (compressor != null);
    if(this.isSetCompressor) {
      this.compressor = compressor;
    }
    this.isSetOffHeap = (offHeap != null);
    if (this.isSetOffHeap) {
      this.offHeap = offHeap;
    }
  }

  // Constructor to be used for supplied region attributes
  public RegionFunctionArgs(String regionPath,
      String useAttributesFrom,
      boolean skipIfExists, String keyConstraint, String valueConstraint,
      Boolean statisticsEnabled, 
      RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime, 
      RegionFunctionArgs.ExpirationAttrs entryExpirationTTL, 
      RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime, 
      RegionFunctionArgs.ExpirationAttrs regionExpirationTTL, String diskStore,
      Boolean diskSynchronous, Boolean enableAsyncConflation,
      Boolean enableSubscriptionConflation, String[] cacheListeners,
      String cacheLoader, String cacheWriter, String[] asyncEventQueueIds,
      String[] gatewaySenderIds, Boolean concurrencyChecksEnabled,
      Boolean cloningEnabled, Integer concurrencyLevel, String prColocatedWith,
      Integer prLocalMaxMemory, Long prRecoveryDelay,
      Integer prRedundantCopies, Long prStartupRecoveryDelay,
      Long prTotalMaxMemory, Integer prTotalNumBuckets, 
      Boolean offHeap,
      Boolean mcastEnabled, RegionAttributes<?, ?> regionAttributes) {   
    this(regionPath, null, useAttributesFrom, skipIfExists, keyConstraint,
        valueConstraint, statisticsEnabled, entryExpirationIdleTime,
        entryExpirationTTL, regionExpirationIdleTime, regionExpirationTTL,
        diskStore, diskSynchronous, enableAsyncConflation,
        enableSubscriptionConflation, cacheListeners, cacheLoader,
        cacheWriter, asyncEventQueueIds, gatewaySenderIds,
        concurrencyChecksEnabled, cloningEnabled, concurrencyLevel, 
        prColocatedWith, prLocalMaxMemory, prRecoveryDelay,
        prRedundantCopies, prStartupRecoveryDelay,
        prTotalMaxMemory, prTotalNumBuckets, null, null, offHeap , mcastEnabled);
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
   * @return the useAttributesFrom
   */
  public String getUseAttributesFrom() {
    return this.useAttributesFrom;
  }

  /**
   * @return true if need to use specified region attributes
   */
  public Boolean isSetUseAttributesFrom() {
    return this.regionShortcut == null && this.useAttributesFrom != null && this.regionAttributes != null;
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
   * @return the isSetStatisticsEnabled
   */
  public Boolean isSetStatisticsEnabled() {
    return this.isSetStatisticsEnabled;
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

  /**
   * @return the isSetDiskSynchronous
   */
  public Boolean isSetDiskSynchronous() {
    return this.isSetDiskSynchronous;
  }
  
  public Boolean isOffHeap() {
    return this.offHeap;
  }

  public Boolean isSetOffHeap() {
    return this.isSetOffHeap;
  }

  /**
   * @return the enableAsyncConflation
   */
  public Boolean isEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  /**
   * @return the isSetEnableAsyncConflation
   */
  public Boolean isSetEnableAsyncConflation() {
    return this.isSetEnableAsyncConflation;
  }

  /**
   * @return the enableSubscriptionConflation
   */
  public Boolean isEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  /**
   * @return the isSetEnableSubscriptionConflation
   */
  public Boolean isSetEnableSubscriptionConflation() {
    return this.isSetEnableSubscriptionConflation;
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
   * @return the isSetConcurrencyChecksEnabled
   */
  public Boolean isSetConcurrencyChecksEnabled() {
    return this.isSetConcurrencyChecksEnabled;
  }

  /**
   * @return the cloningEnabled
   */
  public Boolean isCloningEnabled() {
    return this.cloningEnabled;
  }

  /**
   * @return the isSetCloningEnabled
   */
  public Boolean isSetCloningEnabled() {
    return this.isSetCloningEnabled;
  }

  /**
   * @return the mcastEnabled setting
   */
  public Boolean isMcastEnabled() {
    return this.mcastEnabled;
  }

  /**
   * @return the isSetCloningEnabled
   */
  public Boolean isSetMcastEnabled() {
    return this.isSetMcastEnabled;
  }

  /**
   * @return the concurrencyLevel
   */
  public Integer getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  /**
   * @return the isSetConcurrencyLevel
   */
  public Boolean isSetConcurrencyLevel() {
    return this.isSetConcurrencyLevel;
  }

  public boolean withPartitioning() {
    return hasPartitionAttributes() || (this.regionShortcut != null && this.regionShortcut.name().startsWith("PARTITION"));
  }

  /**
   * @return the partitionArgs
   */
  public boolean hasPartitionAttributes() {
    return this.partitionArgs != null && this.partitionArgs.hasPartitionAttributes();
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
   * @return the isSetCompressor.
   */
  public boolean isSetCompressor() {
    return this.isSetCompressor;
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
  
    private ExpirationFor type;
    private Integer   time;
    private ExpirationAction action;
  
    public ExpirationAttrs(ExpirationFor type, Integer time, String action) {
      this.type   = type;
      this.time   = time;
      if (action != null) {
        this.action = getExpirationAction(action);
      }
    }
  
    public ExpirationAttributes convertToExpirationAttributes() {
      ExpirationAttributes expirationAttr = null;
      if (action != null) {
        expirationAttr = new ExpirationAttributes(time, action);
      } else {
        expirationAttr = new ExpirationAttributes(time);
      }
      return expirationAttr;
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
      return time;
    }
  
    /**
     * @return the action
     */
    public ExpirationAction getAction() {
      return action;
    }
  
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(ExpirationAttrs.class.getSimpleName() + " [type=");
      builder.append(type);
      builder.append(", time=");
      builder.append(time);
      builder.append(", action=");
      builder.append(action);
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
      } else if (action.equalsIgnoreCase(ExpirationAction.INVALIDATE
          .toString())) {
        return ExpirationAction.INVALIDATE;
      } else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_DESTROY
          .toString())) {
        return ExpirationAction.LOCAL_DESTROY;
      } else if (action.equalsIgnoreCase(ExpirationAction.LOCAL_INVALIDATE
          .toString())) {
        return ExpirationAction.LOCAL_INVALIDATE;
      } else {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__EXPIRATION_ACTION_0_IS_NOT_VALID, new Object[] {action}));
      }
    }
  
    public static enum ExpirationFor {
      REGION_IDLE, REGION_TTL, ENTRY_IDLE, ENTRY_TTL;
    }
  }
  
  public static class PartitionArgs implements Serializable {
    private static final long serialVersionUID = 5907052187323280919L;

    private final String prColocatedWith;
    private int prLocalMaxMemory;
    private final boolean isSetPRLocalMaxMemory;
    private long prRecoveryDelay;
    private final boolean isSetPRRecoveryDelay;
    private int prRedundantCopies;
    private final boolean isSetPRRedundantCopies;
    private long prStartupRecoveryDelay;
    private final boolean isSetPRStartupRecoveryDelay;
    private long prTotalMaxMemory;
    private final boolean isSetPRTotalMaxMemory;
    private int prTotalNumBuckets;
    private final boolean isSetPRTotalNumBuckets;

    private boolean hasPartitionAttributes;
    private final Set<String> userSpecifiedPartitionAttributes = new HashSet<String>();

    public PartitionArgs(String prColocatedWith,
        Integer prLocalMaxMemory, Long prRecoveryDelay,
        Integer prRedundantCopies, Long prStartupRecoveryDelay,
        Long prTotalMaxMemory, Integer prTotalNumBuckets) {
      this.prColocatedWith = prColocatedWith;
      if (this.prColocatedWith != null) {
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__COLOCATEDWITH);
      }
      this.isSetPRLocalMaxMemory = prLocalMaxMemory != null;
      if (this.isSetPRLocalMaxMemory) {
        this.prLocalMaxMemory = prLocalMaxMemory;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__LOCALMAXMEMORY);
      }
      this.isSetPRRecoveryDelay = prRecoveryDelay != null;
      if (this.isSetPRRecoveryDelay) {
        this.prRecoveryDelay = prRecoveryDelay;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__RECOVERYDELAY);
      }
      this.isSetPRRedundantCopies = prRedundantCopies != null;
      if (this.isSetPRRedundantCopies) {
        this.prRedundantCopies = prRedundantCopies;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__REDUNDANTCOPIES);
      }
      this.isSetPRStartupRecoveryDelay = prStartupRecoveryDelay != null;
      if (this.isSetPRStartupRecoveryDelay) {
        this.prStartupRecoveryDelay = prStartupRecoveryDelay;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY);
      }
      this.isSetPRTotalMaxMemory = prTotalMaxMemory != null;
      if (this.isSetPRTotalMaxMemory) {
        this.prTotalMaxMemory = prTotalMaxMemory;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__TOTALMAXMEMORY);
      }
      this.isSetPRTotalNumBuckets = prTotalNumBuckets != null;
      if (this.isSetPRTotalNumBuckets) {
        this.prTotalNumBuckets = prTotalNumBuckets;
        this.hasPartitionAttributes = true;
        userSpecifiedPartitionAttributes.add(CliStrings.CREATE_REGION__TOTALNUMBUCKETS);
      }
    }

    /**
     * @return the hasPartitionAttributes
     */
    public Boolean hasPartitionAttributes() {
      return hasPartitionAttributes;
    }

    /**
     * @return the userSpecifiedPartitionAttributes
     */
    public String getUserSpecifiedPartitionAttributes() {
      return CliUtil.collectionToString(userSpecifiedPartitionAttributes, -1);
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
     * @return the isSetPRLocalMaxMemory
     */
    public Boolean isSetPRLocalMaxMemory() {
      return isSetPRLocalMaxMemory;
    }

    /**
     * @return the prRecoveryDelay
     */
    public Long getPrRecoveryDelay() {
      return prRecoveryDelay;
    }

    /**
     * @return the isSetPRRecoveryDelay
     */
    public Boolean isSetPRRecoveryDelay() {
      return isSetPRRecoveryDelay;
    }

    /**
     * @return the prRedundantCopies
     */
    public Integer getPrRedundantCopies() {
      return prRedundantCopies;
    }

    /**
     * @return the isSetPRRedundantCopies
     */
    public Boolean isSetPRRedundantCopies() {
      return isSetPRRedundantCopies;
    }

    /**
     * @return the prStartupRecoveryDelay
     */
    public Long getPrStartupRecoveryDelay() {
      return prStartupRecoveryDelay;
    }

    /**
     * @return the isSetPRStartupRecoveryDelay
     */
    public Boolean isSetPRStartupRecoveryDelay() {
      return isSetPRStartupRecoveryDelay;
    }

    /**
     * @return the prTotalMaxMemory
     */
    public Long getPrTotalMaxMemory() {
      return prTotalMaxMemory;
    }

    /**
     * @return the isSetPRTotalMaxMemory
     */
    public Boolean isSetPRTotalMaxMemory() {
      return isSetPRTotalMaxMemory;
    }

    /**
     * @return the prTotalNumBuckets
     */
    public Integer getPrTotalNumBuckets() {
      return prTotalNumBuckets;
    }

    /**
     * @return the isSetPRTotalNumBuckets
     */
    public Boolean isSetPRTotalNumBuckets() {
      return isSetPRTotalNumBuckets;
    }
  }
}
