/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class HDFSStoreMutatorImpl implements HDFSStoreMutator {
  private HDFSStoreConfigHolder configHolder;
  private Boolean autoCompact;
  private Boolean autoMajorCompact;

  public HDFSStoreMutatorImpl() {
    configHolder = new HDFSStoreConfigHolder();
    configHolder.resetDefaultValues();
  }

  public HDFSStoreMutatorImpl(HDFSStore store) {
    configHolder = new HDFSStoreConfigHolder(store);
  }
  
  public HDFSStoreMutator setWriteOnlyFileRolloverSize(int maxFileSize) {
    configHolder.setWriteOnlyFileRolloverSize(maxFileSize);
    return this;
  }
  @Override
  public int getWriteOnlyFileRolloverSize() {
    return configHolder.getWriteOnlyFileRolloverSize();
  }

  @Override
  public HDFSStoreMutator setWriteOnlyFileRolloverInterval(int count) {
    configHolder.setWriteOnlyFileRolloverInterval(count);
    return this;
  }
  @Override
  public int getWriteOnlyFileRolloverInterval() {
    return configHolder.getWriteOnlyFileRolloverInterval();
  }

  @Override
  public HDFSStoreMutator setMinorCompaction(boolean auto) {
    autoCompact = Boolean.valueOf(auto);
    configHolder.setMinorCompaction(auto);
    return null;
  }
  @Override
  public Boolean getMinorCompaction() {
    return autoCompact;
  }
  
  @Override
  public HDFSStoreMutator setMinorCompactionThreads(int count) {
    configHolder.setMinorCompactionThreads(count);
    return this;
  }
  @Override
  public int getMinorCompactionThreads() {
    return configHolder.getMinorCompactionThreads();
  }
  
  @Override
  public HDFSStoreMutator setMajorCompaction(boolean auto) {
    autoMajorCompact = Boolean.valueOf(auto);
    configHolder.setMajorCompaction(auto);
    return this;
  }
  @Override
  public Boolean getMajorCompaction() {
    return autoMajorCompact;
  }

  @Override
  public HDFSStoreMutator setMajorCompactionInterval(int count) {
    configHolder.setMajorCompactionInterval(count);
    return this;
  }
  @Override
  public int getMajorCompactionInterval() {
    return configHolder.getMajorCompactionInterval();
  }

  @Override
  public HDFSStoreMutator setMajorCompactionThreads(int count) {
    configHolder.setMajorCompactionThreads(count);
    return this;
  }
  @Override
  public int getMajorCompactionThreads() {
    return configHolder.getMajorCompactionThreads();
  }

  @Override
  public HDFSStoreMutator setInputFileSizeMax(int size) {
    configHolder.setInputFileSizeMax(size);
    return this;
  }
  @Override
  public int getInputFileSizeMax() {
    return configHolder.getInputFileSizeMax();
  }
  
  @Override
  public HDFSStoreMutator setInputFileCountMin(int count) {
    configHolder.setInputFileCountMin(count);
    return this;
  }
  @Override
  public int getInputFileCountMin() {
    return configHolder.getInputFileCountMin();
  }
  
  @Override
  public HDFSStoreMutator setInputFileCountMax(int count) {
    configHolder.setInputFileCountMax(count);
    return this;
  }
  @Override
  public int getInputFileCountMax() {
    return configHolder.getInputFileCountMax();
  }
  
  @Override
  public HDFSStoreMutator setPurgeInterval(int interval) {
    configHolder.setPurgeInterval(interval);
    return this;
  }
  @Override
  public int getPurgeInterval() {
    return configHolder.getPurgeInterval();
  }

  @Override
  public int getBatchSize() {
    return configHolder.batchSize;
  }
  @Override
  public HDFSStoreMutator setBatchSize(int size) {
    configHolder.setBatchSize(size);
    return this;
  }

  
  @Override
  public int getBatchInterval() {
    return configHolder.batchIntervalMillis;
  }
  @Override
  public HDFSStoreMutator setBatchInterval(int interval) {
    configHolder.setBatchInterval(interval);
    return this;
  }
    
  public static void assertIsPositive(String name, int count) {
    if (count < 1) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE
              .toLocalizedString(new Object[] { name, count }));
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("HDFSStoreMutatorImpl [");
    if (configHolder != null) {
      builder.append("configHolder=");
      builder.append(configHolder);
      builder.append(", ");
    }
    if (autoCompact != null) {
      builder.append("MinorCompaction=");
      builder.append(autoCompact);
      builder.append(", ");
    }
    if (getMajorCompaction() != null) {
      builder.append("autoMajorCompaction=");
      builder.append(getMajorCompaction());
      builder.append(", ");
    }
    builder.append("]");
    return builder.toString();
  }
}