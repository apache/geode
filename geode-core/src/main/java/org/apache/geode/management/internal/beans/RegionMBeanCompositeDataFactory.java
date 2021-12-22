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
package org.apache.geode.management.internal.beans;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.management.EvictionAttributesData;
import org.apache.geode.management.FixedPartitionAttributesData;
import org.apache.geode.management.MembershipAttributesData;
import org.apache.geode.management.PartitionAttributesData;
import org.apache.geode.management.RegionAttributesData;
import org.apache.geode.management.internal.ManagementConstants;

/**
 * Utility class to create CompositeDataTypes for RegionMXBean
 *
 *
 */
public class RegionMBeanCompositeDataFactory {

  public static EvictionAttributesData getEvictionAttributesData(RegionAttributes regAttrs) {

    String algorithm = "";
    Integer maximum = null;
    if (regAttrs.getEvictionAttributes().getAlgorithm() != null) {
      algorithm = regAttrs.getEvictionAttributes().getAlgorithm().toString();

      if (algorithm.equals(EvictionAlgorithm.NONE.toString())) {
        EvictionAttributesData evictionAttributesData =
            new EvictionAttributesData(algorithm, null, EvictionAlgorithm.NONE.toString());
        return evictionAttributesData;
      }

      if (!regAttrs.getEvictionAttributes().getAlgorithm().isLRUHeap()) {
        maximum = regAttrs.getEvictionAttributes().getMaximum();
      }

    }

    String action = regAttrs.getEvictionAttributes().getAction().toString();
    EvictionAttributesData evictionAttributesData =
        new EvictionAttributesData(algorithm, maximum, action);
    return evictionAttributesData;
  }

  public static MembershipAttributesData getMembershipAttributesData(RegionAttributes regAttrs) {
    MembershipAttributes memAttrs = regAttrs.getMembershipAttributes();
    Set<String> requiredRoles = new HashSet<>();
    for (final Role role : memAttrs.getRequiredRoles()) {
      requiredRoles.add(role.getName());
    }
    String lossAction = memAttrs.getLossAction().toString();

    String resumptionAction = memAttrs.getResumptionAction().toString();

    MembershipAttributesData membershipAttributesData =
        new MembershipAttributesData(requiredRoles, lossAction, resumptionAction);

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
        partitionListeners[i] =
            (partAttrs.getPartitionListeners())[i].getClass().getCanonicalName();
      }
    }

    PartitionAttributesData partitionAttributesData = new PartitionAttributesData(redundantCopies,
        totalMaxMemory, totalNumBuckets, localMaxMemory, colocatedWith, partitionResolver,
        recoveryDelay, startupRecoveryDelay, partitionListeners);

    return partitionAttributesData;
  }

  public static FixedPartitionAttributesData[] getFixedPartitionAttributesData(
      PartitionAttributes partAttrs) {

    FixedPartitionAttributesData[] fixedPartitionAttributesTable =
        new FixedPartitionAttributesData[partAttrs.getFixedPartitionAttributes().size()];
    Iterator<FixedPartitionAttributes> it = partAttrs.getFixedPartitionAttributes().iterator();
    int j = 0;
    while (it.hasNext()) {
      FixedPartitionAttributes fa = it.next();
      FixedPartitionAttributesData data = new FixedPartitionAttributesData(fa.getPartitionName(),
          fa.isPrimary(), fa.getNumBuckets());
      fixedPartitionAttributesTable[j] = data;
      j++;
    }

    return fixedPartitionAttributesTable;
  }

  public static RegionAttributesData getRegionAttributesData(RegionAttributes<?, ?> regAttrs) {

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

    if (listeners != null && listeners.length > 0) {
      cacheListeners = new String[listeners.length];
      int j = 0;
      for (CacheListener l : listeners) {
        cacheListeners[j] = l.getClass().getName();
        j++;
      }
    } else {
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
    boolean offheap = regAttrs.getOffHeap();

    Set<String> eventQueueIds = regAttrs.getAsyncEventQueueIds();
    Set<String> gatewaySenderIds = regAttrs.getGatewaySenderIds();

    RegionAttributesData regionAttributesData = new RegionAttributesData(cacheLoaderClassName,
        cacheWriteClassName, keyConstraintClassName, valueContstraintClassName, regionTimeToLive,
        regionIdleTimeout, entryTimeToLive, entryIdleTimeout, customEntryTimeToLive,
        customEntryIdleTimeout, ignoreJTA, dataPolicy, scope, initialCapacity, loadFactor,
        lockGrantor, multicastEnabled, concurrencyLevel, indexMaintenanceSynchronous,
        statisticsEnabled, subsciptionConflationEnabled, asyncConflationEnabled, poolName,
        isCloningEnabled, diskStoreName, interestPolicy, diskSynchronus, cacheListeners,
        compressorClassName, offheap, eventQueueIds, gatewaySenderIds);

    return regionAttributesData;
  }

}
