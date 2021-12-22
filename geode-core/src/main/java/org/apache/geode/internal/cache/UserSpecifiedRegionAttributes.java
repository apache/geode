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
package org.apache.geode.internal.cache;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.Assert;

/**
 * UserSpecifiedRegionAttributes provides an way to detect departures from default attribute values.
 * It may be used when collecting attributes from an XML parser or from attribute changes made using
 * the {@link org.apache.geode.cache.AttributesFactory}. Its initial usage was to validate when a
 * user set a value which should not be set (for PartitionedRegions).
 *
 * @since GemFire 5.1
 *
 */
public abstract class UserSpecifiedRegionAttributes<K, V> implements RegionAttributes<K, V> {

  /** The attributes' cache listener */
  private boolean hasCacheListeners = false;
  private boolean hasGatewaySenderId = false;
  private boolean hasAsyncEventListeners = false;
  private boolean hasCacheLoader = false;
  private boolean hasCacheWriter = false;
  private boolean hasEntryIdleTimeout = false;
  private boolean hasCustomEntryIdleTimeout = false;
  private boolean hasEntryTimeToLive = false;
  private boolean hasCustomEntryTimeToLive = false;
  private boolean hasInitialCapacity = false;
  private boolean hasKeyConstraint = false;
  private boolean hasValueConstraint = false;
  private boolean hasLoadFactor = false;
  private boolean hasRegionIdleTimeout = false;
  private boolean hasRegionTimeToLive = false;
  private boolean hasScope = false;
  private boolean hasStatisticsEnabled = false;
  private boolean hasIgnoreJTA = false;
  private boolean hasIsLockGrantor = false;
  private boolean hasConcurrencyLevel = false;
  private boolean hasConcurrencyChecksEnabled = false;
  private boolean hasEarlyAck = false;
  private boolean hasMulticastEnabled = false;
  private boolean hasDiskWriteAttributes = false;
  private boolean hasDiskDirs = false;
  private boolean hasDataPolicy = false;
  private boolean hasPartitionAttributes = false;
  private boolean hasMembershipAttributes = false;
  private boolean hasSubscriptionAttributes = false;
  private boolean hasEvictionAttributes = false;
  private boolean hasCustomEviction = false;

  /**
   * Whether this region has specified a disk store name
   *
   * @since GemFire prPersistSprint2
   */
  private boolean hasDiskStoreName = false;
  private boolean hasDiskSynchronous = false;

  /**
   * Whether this region has publisher explicitly set
   *
   * @since GemFire 4.2.3
   */

  private boolean hasPublisher = false;
  /**
   * Whether this region has enable bridge conflation explicitly set
   *
   * @since GemFire 4.2
   */
  private boolean hasEnableSubscriptionConflation = false;
  /**
   * Whether this region has enable async conflation explicitly set
   *
   * @since GemFire 4.2.3
   */
  private boolean hasEnableAsyncConflation = false;


  private boolean hasIndexMaintenanceSynchronous = false;


  /**
   * Whether this region has a client to server connection pool
   *
   * @since GemFire 5.7
   */
  private boolean hasPoolName = false;
  /**
   * Whether this region has a cloning enabled
   *
   * @since GemFire 6.1
   */
  private boolean hasCloningEnabled = false;

  /**
   * Whether this region has entry value compression.
   *
   * @since GemFire 8.0
   */
  private boolean hasCompressor = false;

  /**
   * Whether this region has enable off-heap memory set.
   *
   * @since Geode 1.0
   */
  private boolean hasOffHeap = false;

  public boolean hasCacheLoader() {
    return hasCacheLoader;
  }

  public boolean hasCacheWriter() {
    return hasCacheWriter;
  }

  public boolean hasKeyConstraint() {
    return hasKeyConstraint;
  }

  public boolean hasValueConstraint() {
    return hasValueConstraint;
  }

  public boolean hasRegionTimeToLive() {
    return hasRegionTimeToLive;
  }

  public boolean hasRegionIdleTimeout() {
    return hasRegionIdleTimeout;
  }

  public boolean hasEntryTimeToLive() {
    return hasEntryTimeToLive;
  }

  public boolean hasCustomEntryTimeToLive() {
    return hasCustomEntryTimeToLive;
  }

  public boolean hasEntryIdleTimeout() {
    return hasEntryIdleTimeout;
  }

  public boolean hasCustomEntryIdleTimeout() {
    return hasCustomEntryIdleTimeout;
  }

  public boolean hasMirrorType() {
    return hasDataPolicy();
  }

  public boolean hasDataPolicy() {
    return hasDataPolicy;
  }

  public boolean hasScope() {
    return hasScope;
  }

  public boolean hasCacheListeners() {
    return hasCacheListeners;
  }

  public boolean hasGatewaySenderId() {
    return hasGatewaySenderId;
  }

  public boolean hasAsyncEventListeners() {
    return hasAsyncEventListeners;
  }

  public boolean hasInitialCapacity() {
    return hasInitialCapacity;
  }

  public boolean hasLoadFactor() {
    return hasLoadFactor;
  }

  public boolean hasConcurrencyLevel() {
    return hasConcurrencyLevel;
  }

  public boolean hasConcurrencyChecksEnabled() {
    return hasConcurrencyChecksEnabled;
  }

  public boolean hasStatisticsEnabled() {
    return hasStatisticsEnabled;
  }

  public boolean hasIgnoreJTA() {
    return hasIgnoreJTA;
  }

  public boolean hasIsLockGrantor() {
    return hasIsLockGrantor;
  }

  public boolean hasPersistBackup() {
    return hasDataPolicy();
  }

  public boolean hasEarlyAck() {
    return hasEarlyAck;
  }

  public boolean hasMulticastEnabled() {
    return hasMulticastEnabled;
  }

  public boolean hasPublisher() {
    return hasPublisher;
  }

  public boolean hasPartitionAttributes() {
    return hasPartitionAttributes;
  }

  public boolean hasSubscriptionAttributes() {
    return hasSubscriptionAttributes;
  }

  public boolean hasEnableSubscriptionConflation() {
    return hasEnableSubscriptionConflation;
  }

  public boolean hasEnableAsyncConflation() {
    return hasEnableAsyncConflation;
  }

  public boolean hasIndexMaintenanceSynchronous() {
    return hasIndexMaintenanceSynchronous;
  }

  public boolean hasDiskWriteAttributes() {
    return hasDiskWriteAttributes;
  }

  public boolean hasDiskDirs() {
    return hasDiskDirs;
  }

  public boolean hasMembershipAttributes() {
    return hasMembershipAttributes;
  }

  public boolean hasEvictionAttributes() {
    return hasEvictionAttributes;
  }

  public boolean hasCustomEviction() {
    return hasCustomEviction;
  }

  public boolean hasPoolName() {
    return hasPoolName;
  }

  public boolean hasCompressor() {
    return hasCompressor;
  }

  public boolean hasOffHeap() {
    return hasOffHeap;
  }

  public boolean hasCloningEnabled() {
    return hasCloningEnabled;
  }

  public boolean hasDiskStoreName() {
    return hasDiskStoreName;
  }

  public boolean hasDiskSynchronous() {
    return hasDiskSynchronous;
  }

  public void setHasCacheListeners(boolean hasCacheListeners) {
    this.hasCacheListeners = hasCacheListeners;
  }

  public void setHasGatewaySenderIds(boolean hasgatewaySenders) {
    hasGatewaySenderId = hasgatewaySenders;
  }

  public void setHasAsyncEventListeners(boolean hasAsyncEventListeners) {
    this.hasAsyncEventListeners = hasAsyncEventListeners;
  }

  public void setHasCacheLoader(boolean hasCacheLoader) {
    this.hasCacheLoader = hasCacheLoader;
  }

  public void setHasCacheWriter(boolean hasCacheWriter) {
    this.hasCacheWriter = hasCacheWriter;
  }

  public void setHasConcurrencyLevel(boolean hasConcurrencyLevel) {
    this.hasConcurrencyLevel = hasConcurrencyLevel;
  }

  public void setHasConcurrencyChecksEnabled(boolean has) {
    hasConcurrencyChecksEnabled = has;
  }

  public void setHasDataPolicy(boolean hasDataPolicy) {
    this.hasDataPolicy = hasDataPolicy;
  }

  public void setHasDiskDirs(boolean hasDiskDirs) {
    this.hasDiskDirs = hasDiskDirs;
  }

  public void setHasDiskWriteAttributes(boolean hasDiskWriteAttributes) {
    this.hasDiskWriteAttributes = hasDiskWriteAttributes;
  }

  public void setHasEarlyAck(boolean hasEarlyAck) {
    this.hasEarlyAck = hasEarlyAck;
  }

  public void setHasEnableAsyncConflation(boolean hasEnableAsyncConflation) {
    this.hasEnableAsyncConflation = hasEnableAsyncConflation;
  }

  public void setHasEnableSubscriptionConflation(boolean hasEnableSubscriptionConflation) {
    this.hasEnableSubscriptionConflation = hasEnableSubscriptionConflation;
  }

  public void setHasEntryIdleTimeout(boolean hasEntryIdleTimeout) {
    this.hasEntryIdleTimeout = hasEntryIdleTimeout;
  }

  public void setHasCustomEntryIdleTimeout(boolean has) {
    hasCustomEntryIdleTimeout = has;
  }

  public void setHasEntryTimeToLive(boolean hasEntryTimeToLive) {
    this.hasEntryTimeToLive = hasEntryTimeToLive;
  }

  public void setHasCustomEntryTimeToLive(boolean has) {
    hasCustomEntryTimeToLive = has;
  }

  public void setHasEvictionAttributes(boolean hasEvictionAttributes) {
    this.hasEvictionAttributes = hasEvictionAttributes;
  }

  public void setHasCustomEviction(boolean hasCustomEviction) {
    this.hasCustomEviction = hasCustomEviction;
  }

  public void setHasIgnoreJTA(boolean hasIgnoreJTA) {
    this.hasIgnoreJTA = hasIgnoreJTA;
  }

  public void setHasIndexMaintenanceSynchronous(boolean hasIndexMaintenanceSynchronous) {
    this.hasIndexMaintenanceSynchronous = hasIndexMaintenanceSynchronous;
  }

  public void setHasInitialCapacity(boolean hasInitialCapacity) {
    this.hasInitialCapacity = hasInitialCapacity;
  }

  public void setHasIsLockGrantor(boolean hasIsLockGrantor) {
    this.hasIsLockGrantor = hasIsLockGrantor;
  }

  public void setHasKeyConstraint(boolean hasKeyConstraint) {
    this.hasKeyConstraint = hasKeyConstraint;
  }

  public void setHasLoadFactor(boolean hasLoadFactor) {
    this.hasLoadFactor = hasLoadFactor;
  }

  public void setHasMembershipAttributes(boolean hasMembershipAttributes) {
    this.hasMembershipAttributes = hasMembershipAttributes;
  }

  public void setHasMulticastEnabled(boolean hasMulticastEnabled) {
    this.hasMulticastEnabled = hasMulticastEnabled;
  }

  public void setHasPartitionAttributes(boolean hasPartitionAttributes) {
    this.hasPartitionAttributes = hasPartitionAttributes;
  }

  public void setHasPublisher(boolean hasPublisher) {
    this.hasPublisher = hasPublisher;
  }

  public void setHasRegionIdleTimeout(boolean hasRegionIdleTimeout) {
    this.hasRegionIdleTimeout = hasRegionIdleTimeout;
  }

  public void setHasRegionTimeToLive(boolean hasRegionTimeToLive) {
    this.hasRegionTimeToLive = hasRegionTimeToLive;
  }

  public void setHasScope(boolean hasScope) {
    this.hasScope = hasScope;
  }

  public void setHasStatisticsEnabled(boolean hasStatisticsEnabled) {
    this.hasStatisticsEnabled = hasStatisticsEnabled;
  }

  public void setHasSubscriptionAttributes(boolean hasSubscriptionAttributes) {
    this.hasSubscriptionAttributes = hasSubscriptionAttributes;
  }

  public void setHasValueConstraint(boolean hasValueConstraint) {
    this.hasValueConstraint = hasValueConstraint;
  }

  /**
   * If set to true then an exception will be thrown at creation time if hasPoolName is not true.
   *
   * @since GemFire 6.5
   */
  public boolean requiresPoolName = false;
  /**
   * Holds index information. Hoisted up to this class in 7.0
   *
   * @since GemFire 6.5
   */
  private List indexes;

  public void setHasPoolName(boolean hasPool) {
    hasPoolName = hasPool;
  }

  public void setHasCompressor(boolean hasCompressor) {
    this.hasCompressor = hasCompressor;
  }

  public void setHasOffHeap(boolean hasOffHeap) {
    this.hasOffHeap = hasOffHeap;
  }

  public void setAllHasFields(boolean b) {
    int hasCounter = 0;
    Field[] thisFields = UserSpecifiedRegionAttributes.class.getDeclaredFields();
    for (final Field thisField : thisFields) {
      if (thisField.getName().startsWith("has")) {
        hasCounter++;
        try {
          thisField.setBoolean(this, b);
        } catch (IllegalAccessException ouch) {
          Assert.assertTrue(false,
              "Could not access field" + thisField.getName() + " on " + getClass());
        }
      }
    }
    Assert.assertTrue(hasCounter == HAS_COUNT, "Found " + hasCounter + " methods");
    // this.hasCacheListeners = b;
    // this.hasCacheLoader = b;
    // this.hasCacheWriter = b;
    // this.hasConcurrencyLevel = b;
    // this.hasDataPolicy = b;
    // this.hasDiskDirs = b;
    // this.hasDiskWriteAttributes = b;
    // this.hasEarlyAck = b;
    // this.hasEnableAsyncConflation = b;
    // this.hasEnableSubscriptionConflation = b;
    // this.hasEnableGateway = b;
    // this.hasEntryIdleTimeout = b;
    // this.hasEntryTimeToLive = b;
    // this.hasEvictionAttributes = b;
    // this.hasIgnoreJTA = b;
    // this.hasIndexMaintenanceSynchronous = b;
    // this.hasInitialCapacity = b;
    // this.hasIsLockGrantor = b;
    // this.hasKeyConstraint = b;
    // this.hasLoadFactor = b;
    // this.hasMembershipAttributes = b;
    // this.hasMulticastEnabled = b;
    // this.hasPartitionAttributes = b;
    // this.hasPublisher = b;
    // this.hasRegionIdleTimeout = b;
    // this.hasRegionTimeToLive = b;
    // this.hasScope = b;
    // this.hasStatisticsEnabled = b;
    // this.hasSubscriptionAttributes = b;
    // this.hasValueConstraint = b;
  }

  public void setHasCloningEnabled(boolean val) {
    hasCloningEnabled = val;
  }

  public void setHasDiskStoreName(boolean val) {
    hasDiskStoreName = val;
  }

  public void setHasDiskSynchronous(boolean val) {
    hasDiskSynchronous = val;
  }

  private static final int HAS_COUNT = 41;

  public void initHasFields(UserSpecifiedRegionAttributes<K, V> other) {
    Field[] thisFields = UserSpecifiedRegionAttributes.class.getDeclaredFields();
    Object[] emptyArgs = new Object[] {};
    int hasCounter = 0;
    String fieldName = null;
    for (final Field thisField : thisFields) {
      fieldName = thisField.getName();
      if (fieldName.startsWith("has")) {
        hasCounter++;
        boolean bval = false;

        try {
          Method otherMeth = other.getClass().getMethod(fieldName/* , (Class[])null */);
          bval = ((Boolean) otherMeth.invoke(other, emptyArgs)).booleanValue();
        } catch (NoSuchMethodException darnit) {
          Assert.assertTrue(false, "A has* method accessor is required for field " + fieldName);
        } catch (IllegalAccessException boom) {
          Assert.assertTrue(false, "Could not access method " + fieldName + " on " + getClass());
        } catch (IllegalArgumentException e) {
          Assert.assertTrue(false,
              "Illegal argument trying to set field " + e.getLocalizedMessage());
        } catch (InvocationTargetException e) {
          Assert.assertTrue(false, "Failed trying to invoke method " + e.getLocalizedMessage());
        }

        try {
          thisField.setBoolean(this, bval);
        } catch (IllegalAccessException ouch) {
          Assert.assertTrue(false, "Could not access field" + fieldName + " on " + getClass());
        }
      }
    }
    Assert.assertTrue(hasCounter == HAS_COUNT,
        "Expected " + HAS_COUNT + " methods, got " + hasCounter + " last field: " + fieldName);

    // this.hasCacheListeners = other.hasCacheListeners();
    // this.hasCacheLoader = other.hasCacheLoader();
    // this.hasCacheWriter = other.hasCacheWriter();
    // this.hasConcurrencyLevel = other.hasConcurrencyLevel();
    // this.hasDataPolicy = other.hasDataPolicy();
    // this.hasDiskDirs = other.hasDiskDirs();
    // this.hasDiskWriteAttributes = other.hasDiskWriteAttributes();
    // this.hasEarlyAck = other.hasEarlyAck();
    // this.hasEnableAsyncConflation = other.hasEnableAsyncConflation();
    // this.hasEnableSubscriptionConflation = other.hasEnableSubscriptionConflation();
    // this.hasEnableGateway = other.hasEnableGateway();
    // this.hasEntryIdleTimeout = other.hasEntryIdleTimeout();
    // this.hasEntryTimeToLive = other.hasEntryTimeToLive();
    // this.hasEvictionAttributes = other.hasEvictionAttributes();
    // this.hasIgnoreJTA = other.hasIgnoreJTA();
    // this.hasIndexMaintenanceSynchronous = other.hasIndexMaintenanceSynchronous();
    // this.hasInitialCapacity = other.hasInitialCapacity();
    // this.hasIsLockGrantor = other.hasIsLockGrantor();
    // this.hasKeyConstraint = other.hasKeyConstraint();
    // this.hasLoadFactor = other.hasLoadFactor();
    // this.hasMembershipAttributes = other.hasMembershipAttributes();
    // this.hasMulticastEnabled = other.hasMulticastEnabled();
    // this.hasPartitionAttributes = other.hasPartitionAttributes();
    // this.hasPublisher = other.hasPublisher();
    // this.hasRegionIdleTimeout = other.hasRegionIdleTimeout();
    // this.hasRegionTimeToLive = other.hasRegionTimeToLive();
    // this.hasScope = other.hasScope();
    // this.hasStatisticsEnabled = other.hasStatisticsEnabled();
    // this.hasSubscriptionAttributes = other.hasSubscriptionAttributes();
    // this.hasValueConstraint = other.hasValueConstraint();
  }

  public void setIndexes(List indexes) {
    this.indexes = indexes;
  }

  public List getIndexes() {
    return indexes;
  }
}
