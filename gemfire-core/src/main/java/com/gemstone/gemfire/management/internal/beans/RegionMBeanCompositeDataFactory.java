/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.FixedPartitionAttributesData;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.internal.ManagementConstants;

/**
 * Utility class to create CompositeDataTypes for RegionMXBean
 * 
 * @author rishim
 *
 */
public class RegionMBeanCompositeDataFactory {

  public static EvictionAttributesData getEvictionAttributesData(RegionAttributes regAttrs) {

    String algorithm = "";
    Integer maximum = null;
    if (regAttrs.getEvictionAttributes().getAlgorithm() != null) {
      algorithm = regAttrs.getEvictionAttributes().getAlgorithm().toString();

      if (algorithm.equals(EvictionAlgorithm.NONE.toString())) {
        EvictionAttributesData evictionAttributesData = new EvictionAttributesData(algorithm, null,
            EvictionAlgorithm.NONE.toString());
        return evictionAttributesData;
      }

      if (!regAttrs.getEvictionAttributes().getAlgorithm().isLRUHeap()) {
        maximum = regAttrs.getEvictionAttributes().getMaximum();
      }

    }

    String action = regAttrs.getEvictionAttributes().getAction().toString();
    EvictionAttributesData evictionAttributesData = new EvictionAttributesData(algorithm, maximum, action);
    return evictionAttributesData;
  }

  public static MembershipAttributesData getMembershipAttributesData(RegionAttributes regAttrs) {
    MembershipAttributes memAttrs = regAttrs.getMembershipAttributes();
    Set<String> requiredRoles = new HashSet<String>();
    Iterator<Role> it = memAttrs.getRequiredRoles().iterator();
    while (it.hasNext()) {
      requiredRoles.add(it.next().getName());
    }
    String lossAction = memAttrs.getLossAction().toString();

    String resumptionAction = memAttrs.getResumptionAction().toString();

    MembershipAttributesData membershipAttributesData = new MembershipAttributesData(requiredRoles, lossAction,
        resumptionAction);

    return membershipAttributesData;

  }

  public static PartitionAttributesData getPartitionAttributesData(PartitionAttributes partAttrs) {

    int redundantCopies = partAttrs.getRedundantCopies();
    long totalMaxMemory = partAttrs.getTotalMaxMemory();
    // Total number of buckets for whole region
    int totalNumBuckets = partAttrs.getTotalNumBuckets();
    int localMaxMemory = partAttrs.getLocalMaxMemory();
    String colocatedWith = partAttrs.getColocatedWith();
    String partitionResolver = null;
    if (partAttrs.getPartitionResolver() != null) {
      partitionResolver = partAttrs.getPartitionResolver().getName();
    }

    long recoveryDelay = partAttrs.getRecoveryDelay();
    long startupRecoveryDelay = partAttrs.getStartupRecoveryDelay();
    String[] partitionListeners = null;
    if (partAttrs.getPartitionListeners() != null) {
      partitionListeners = new String[partAttrs.getPartitionListeners().length];
      for (int i = 0; i < partAttrs.getPartitionListeners().length; i++) {
        partitionListeners[i] = (partAttrs.getPartitionListeners())[i].getClass().getCanonicalName();
      }
    }

    PartitionAttributesData partitionAttributesData = new PartitionAttributesData(redundantCopies, totalMaxMemory,
        totalNumBuckets, localMaxMemory, colocatedWith, partitionResolver, recoveryDelay, startupRecoveryDelay,
        partitionListeners);

    return partitionAttributesData;
  }

  public static FixedPartitionAttributesData[] getFixedPartitionAttributesData(PartitionAttributes partAttrs) {

    FixedPartitionAttributesData[] fixedPartitionAttributesTable = new FixedPartitionAttributesData[partAttrs
        .getFixedPartitionAttributes().size()];
    Iterator<FixedPartitionAttributes> it = partAttrs.getFixedPartitionAttributes().iterator();
    int j = 0;
    while (it.hasNext()) {
      FixedPartitionAttributes fa = it.next();
      FixedPartitionAttributesData data = new FixedPartitionAttributesData(fa.getPartitionName(), fa.isPrimary(), fa
          .getNumBuckets());
      fixedPartitionAttributesTable[j] = data;
      j++;
    }

    return fixedPartitionAttributesTable;
  }

  public static RegionAttributesData getRegionAttributesData(RegionAttributes regAttrs) {

    String cacheLoaderClassName = null;
    if (regAttrs.getCacheLoader() != null) {
      cacheLoaderClassName = regAttrs.getCacheLoader().getClass().getCanonicalName();
    }
    String cacheWriteClassName = null;
    if (regAttrs.getCacheWriter() != null) {
      cacheWriteClassName = regAttrs.getCacheWriter().getClass().getCanonicalName();
    }
    String keyConstraintClassName = null;
    if (regAttrs.getKeyConstraint() != null) {
      keyConstraintClassName = regAttrs.getKeyConstraint().getName();
    }

    String valueContstraintClassName = null;
    if (regAttrs.getValueConstraint() != null) {
      valueContstraintClassName = regAttrs.getValueConstraint().getName();
    }

    CacheListener[] listeners = regAttrs.getCacheListeners();
    
    String[] cacheListeners = null;

    if(listeners != null && listeners.length > 0){
      cacheListeners = new String[listeners.length];
      int j = 0;
      for(CacheListener l : listeners){
        cacheListeners[j] = l.getClass().getName();
        j++;
      }
    }else{
      cacheListeners = ManagementConstants.NO_DATA_STRING;
    }
    


    int regionTimeToLive = regAttrs.getRegionTimeToLive().getTimeout();
    int regionIdleTimeout = regAttrs.getRegionIdleTimeout().getTimeout();
    int entryTimeToLive = regAttrs.getEntryTimeToLive().getTimeout();
    int entryIdleTimeout = regAttrs.getEntryIdleTimeout().getTimeout();

    String customEntryTimeToLive = null;
    Object o1 = regAttrs.getCustomEntryTimeToLive();
    if (o1 != null) {
      customEntryTimeToLive = o1.toString();
    }

    String customEntryIdleTimeout = null;
    Object o2 = regAttrs.getCustomEntryIdleTimeout();
    if (o2 != null) {
      customEntryIdleTimeout = o2.toString();
    }

    boolean ignoreJTA = regAttrs.getIgnoreJTA();
    String dataPolicy = regAttrs.getDataPolicy().toString();
    String scope = regAttrs.getScope().toString();
    int initialCapacity = regAttrs.getInitialCapacity();
    float loadFactor = regAttrs.getLoadFactor();
    boolean lockGrantor = regAttrs.isLockGrantor();
    boolean multicastEnabled = regAttrs.getMulticastEnabled();
    int concurrencyLevel = regAttrs.getConcurrencyLevel();
    boolean indexMaintenanceSynchronous = regAttrs.getIndexMaintenanceSynchronous();
    boolean statisticsEnabled = regAttrs.getStatisticsEnabled();

    boolean subsciptionConflationEnabled = regAttrs.getEnableSubscriptionConflation();
    boolean asyncConflationEnabled = regAttrs.getEnableAsyncConflation();
    String poolName = regAttrs.getPoolName();
    boolean isCloningEnabled = regAttrs.getCloningEnabled();
    String diskStoreName = regAttrs.getDiskStoreName();
    String interestPolicy = null;
    if (regAttrs.getSubscriptionAttributes() != null) {
      interestPolicy = regAttrs.getSubscriptionAttributes().getInterestPolicy().toString();
    }

    String compressorClassName = null;
    if (regAttrs.getCompressor() != null) {
      compressorClassName = regAttrs.getCompressor().getClass().getCanonicalName();
    }

    boolean diskSynchronus = regAttrs.isDiskSynchronous();


    RegionAttributesData regionAttributesData = new RegionAttributesData(cacheLoaderClassName, cacheWriteClassName, keyConstraintClassName,
        valueContstraintClassName, regionTimeToLive, regionIdleTimeout, entryTimeToLive, entryIdleTimeout, customEntryTimeToLive,
        customEntryIdleTimeout, ignoreJTA, dataPolicy, scope, initialCapacity, loadFactor, lockGrantor, multicastEnabled, concurrencyLevel,
        indexMaintenanceSynchronous, statisticsEnabled, subsciptionConflationEnabled,
        asyncConflationEnabled, poolName, isCloningEnabled, diskStoreName, interestPolicy, diskSynchronus, cacheListeners,
        compressorClassName);


    return regionAttributesData;
  }

}
