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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * Used to carry arguments between gfsh region command implementations and the functions that do the
 * work for those commands.
 *
 * @since GemFire 7.0
 */
public class RegionFunctionArgs implements Serializable {
  private static final long serialVersionUID = 2204943186081037302L;

  private String regionPath;
  private RegionShortcut regionShortcut;
  private String templateRegion;
  private boolean ifNotExists;
  private String keyConstraint;
  private String valueConstraint;
  private Boolean statisticsEnabled;
  private ExpirationAttrs entryExpirationIdleTime;
  private ExpirationAttrs entryExpirationTTL;
  private ClassName<CustomExpiry> entryIdleTimeCustomExpiry;
  private ClassName<CustomExpiry> entryTTLCustomExpiry;
  private ExpirationAttrs regionExpirationIdleTime;
  private ExpirationAttrs regionExpirationTTL;
  private EvictionAttrs evictionAttributes;
  private String diskStore;
  private Boolean diskSynchronous;
  private Boolean enableAsyncConflation;
  private Boolean enableSubscriptionConflation;
  private Set<ClassName<CacheListener>> cacheListeners = Collections.emptySet();
  private ClassName<CacheLoader> cacheLoader;
  private ClassName<CacheWriter> cacheWriter;
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

  public RegionFunctionArgs() {}

  public void setRegionPath(String regionPath) {
    this.regionPath = regionPath;
  }

  public void setTemplateRegion(String templateRegion) {
    this.templateRegion = templateRegion;
  }

  public void setRegionShortcut(RegionShortcut regionShortcut) {
    this.regionShortcut = regionShortcut;
  }


  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
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

  public void setEntryExpirationIdleTime(Integer timeout, ExpirationAction action) {
    if (timeout == null && action == null) {
      return;
    }

    this.entryExpirationIdleTime = new ExpirationAttrs(timeout, action);
  }

  public void setEntryExpirationTTL(Integer timeout, ExpirationAction action) {
    if (timeout == null && action == null) {
      return;
    }

    this.entryExpirationTTL = new ExpirationAttrs(timeout, action);
  }

  public void setRegionExpirationIdleTime(Integer timeout, ExpirationAction action) {
    if (timeout == null && action == null) {
      return;
    }

    this.regionExpirationIdleTime = new ExpirationAttrs(timeout, action);
  }

  public void setRegionExpirationTTL(Integer timeout, ExpirationAction action) {
    if (timeout == null && action == null) {
      return;
    }

    this.regionExpirationTTL = new ExpirationAttrs(timeout, action);
  }

  public void setEvictionAttributes(String action, Integer maxMemory, Integer maxEntryCount,
      String objectSizer) {
    if (action == null) {
      return;
    }

    this.evictionAttributes = new EvictionAttrs(action, maxEntryCount, maxMemory, objectSizer);
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

  public void setCacheListeners(ClassName<CacheListener>[] cacheListeners) {
    if (cacheListeners != null) {
      this.cacheListeners = Arrays.stream(cacheListeners).collect(Collectors.toSet());
    }
  }

  public void setCacheLoader(ClassName<CacheLoader> cacheLoader) {
    this.cacheLoader = cacheLoader;
  }

  public void setCacheWriter(ClassName<CacheWriter> cacheWriter) {
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
    if (prColocatedWith == null &&
        prLocalMaxMemory == null &&
        prRecoveryDelay == null &&
        prRedundantCopies == null &&
        prStartupRecoveryDelay == null &&
        prTotalMaxMemory == null &&
        prTotalNumBuckets == null &&
        partitionResolver == null) {
      return;
    }
    if (partitionArgs == null) {
      partitionArgs = new PartitionArgs();
    }
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
   * @return the ifNotExists
   */
  public Boolean isIfNotExists() {
    return this.ifNotExists;
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
  public Boolean getStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  /**
   * @return the entryExpirationIdleTime
   */
  public ExpirationAttrs getEntryExpirationIdleTime() {
    return this.entryExpirationIdleTime;
  }

  /**
   * @return the entryExpirationTTL
   */
  public ExpirationAttrs getEntryExpirationTTL() {
    return this.entryExpirationTTL;
  }

  /**
   * @return the regionExpirationIdleTime
   */
  public ExpirationAttrs getRegionExpirationIdleTime() {
    return this.regionExpirationIdleTime;
  }

  /**
   * @return the regionExpirationTTL
   */
  public ExpirationAttrs getRegionExpirationTTL() {
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
  public Boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  public Boolean getOffHeap() {
    return this.offHeap;
  }

  /**
   * @return the enableAsyncConflation
   */
  public Boolean getEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  /**
   * @return the enableSubscriptionConflation
   */
  public Boolean getEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  /**
   * @return the cacheListeners
   */
  public Set<ClassName<CacheListener>> getCacheListeners() {
    if (this.cacheListeners == null) {
      return null;
    }
    return Collections.unmodifiableSet(this.cacheListeners);
  }

  /**
   * @return the cacheLoader
   */
  public ClassName<CacheLoader> getCacheLoader() {
    return this.cacheLoader;
  }

  /**
   * @return the cacheWriter
   */
  public ClassName<CacheWriter> getCacheWriter() {
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
  public Boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  /**
   * @return the cloningEnabled
   */
  public Boolean getCloningEnabled() {
    return this.cloningEnabled;
  }

  /**
   * @return the mcastEnabled setting
   */
  public Boolean getMcastEnabled() {
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

  public EvictionAttrs getEvictionAttributes() {
    return this.evictionAttributes;
  }

  /**
   * @return the regionAttributes
   */
  @SuppressWarnings("unchecked")
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    return (RegionAttributes<K, V>) this.regionAttributes;
  }

  public ClassName<CustomExpiry> getEntryIdleTimeCustomExpiry() {
    return entryIdleTimeCustomExpiry;
  }

  public void setEntryIdleTimeCustomExpiry(ClassName<CustomExpiry> entryIdleTimeCustomExpiry) {
    this.entryIdleTimeCustomExpiry = entryIdleTimeCustomExpiry;
  }

  public ClassName<CustomExpiry> getEntryTTLCustomExpiry() {
    return entryTTLCustomExpiry;
  }

  public void setEntryTTLCustomExpiry(ClassName<CustomExpiry> entryTTLCustomExpiry) {
    this.entryTTLCustomExpiry = entryTTLCustomExpiry;
  }

  /**
   * the difference between this and ExpirationAttributes is that this allows time and action to be
   * null
   */
  public static class ExpirationAttrs implements Serializable {
    private static final long serialVersionUID = 1474255033398008063L;

    private final Integer time;
    private final ExpirationAction action;

    public ExpirationAttrs(Integer time, ExpirationAction action) {
      this.time = time;
      this.action = action;
    }

    public Integer getTime() {
      return time;
    }

    public ExpirationAction getAction() {
      return action;
    }

    @Override
    public int hashCode() {
      return Objects.hash(time, action);
    }

    @Override
    public boolean equals(Object object) {
      if (object == null) {
        return false;
      }

      if (!(object instanceof ExpirationAttrs)) {
        return false;
      }

      ExpirationAttrs that = (ExpirationAttrs) object;
      if (time == null && that.time == null && action == that.action) {
        return true;
      }
      return time != null && time.equals(that.time) && action == that.action;
    }

    public boolean isTimeSet() {
      return time != null;
    }

    public boolean isTimeOrActionSet() {
      return time != null || action != null;
    }

    public ExpirationAttributes getExpirationAttributes() {
      return getExpirationAttributes(null);
    }

    public ExpirationAttributes getExpirationAttributes(ExpirationAttributes existing) {
      // default values
      int timeToUse = 0;
      ExpirationAction actionToUse = ExpirationAction.INVALIDATE;

      if (existing != null) {
        timeToUse = existing.getTimeout();
        actionToUse = existing.getAction();
      }
      if (time != null) {
        timeToUse = time;
      }

      if (action != null) {
        actionToUse = action;
      }
      return new ExpirationAttributes(timeToUse, actionToUse);
    }
  }

  public static class EvictionAttrs implements Serializable {
    private static final long serialVersionUID = 9015454906371076014L;

    private String evictionAction;
    private Integer maxEntryCount;
    private Integer maxMemory;
    private String objectSizer;

    public EvictionAttrs(String evictionAction, Integer maxEntryCount, Integer maxMemory,
        String objectSizer) {
      this.evictionAction = evictionAction;
      this.maxEntryCount = maxEntryCount;
      this.maxMemory = maxMemory;
      this.objectSizer = objectSizer;
    }

    public String getEvictionAction() {
      return evictionAction;
    }

    public Integer getMaxEntryCount() {
      return maxEntryCount;
    }

    public Integer getMaxMemory() {
      return maxMemory;
    }

    public String getObjectSizer() {
      return objectSizer;
    }

    public EvictionAttributes convertToEvictionAttributes() {
      EvictionAction action = EvictionAction.parseAction(evictionAction);

      ObjectSizer sizer;
      if (objectSizer != null) {
        try {
          Class<ObjectSizer> sizerClass =
              (Class<ObjectSizer>) ClassPathLoader.getLatest().forName(objectSizer);
          sizer = sizerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new IllegalArgumentException(
              "Unable to instantiate class " + objectSizer + " - " + e.toString());
        }
      } else {
        sizer = ObjectSizer.DEFAULT;
      }

      if (maxMemory == null && maxEntryCount == null) {
        return EvictionAttributes.createLRUHeapAttributes(sizer, action);
      } else if (maxMemory != null) {
        return EvictionAttributes.createLRUMemoryAttributes(maxMemory, sizer, action);
      } else {
        return EvictionAttributes.createLRUEntryAttributes(maxEntryCount, action);
      }
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
