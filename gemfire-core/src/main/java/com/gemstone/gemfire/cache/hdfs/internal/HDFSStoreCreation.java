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

package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.StoreExistsException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author ashvina
 */
public class HDFSStoreCreation implements HDFSStoreFactory {
  protected HDFSStoreConfigHolder configHolder;
  
  public HDFSStoreCreation() {
    this(null);
  }

  /**
   * Copy constructor for HDFSStoreCreation
   * @param config configuration source for creating this instance 
   */
  public HDFSStoreCreation(HDFSStoreCreation config) {
    this.configHolder = new HDFSStoreConfigHolder(config == null ? null : config.configHolder);
  }

  @Override
  public HDFSStoreFactory setName(String name) {
    configHolder.setName(name);
    return this;
  }

  @Override
  public HDFSStoreFactory setNameNodeURL(String namenodeURL) {
    configHolder.setNameNodeURL(namenodeURL);
    return this;
  }

  @Override
  public HDFSStoreFactory setHomeDir(String homeDir) {
    configHolder.setHomeDir(homeDir);
    return this;
  }

  @Override
  public HDFSStoreFactory setHDFSClientConfigFile(String clientConfigFile) {
    configHolder.setHDFSClientConfigFile(clientConfigFile);
    return this;
  }
  
  @Override
  public HDFSStoreFactory setBlockCacheSize(float percentage) {
    configHolder.setBlockCacheSize(percentage);
    return this;
  }
  
  @Override
  public HDFSStoreFactory setWriteOnlyFileRolloverSize(int maxFileSize) {
    configHolder.setWriteOnlyFileRolloverSize(maxFileSize);
    return this;
  }

  @Override
  public HDFSStoreFactory setWriteOnlyFileRolloverInterval(int count) {
    configHolder.setWriteOnlyFileRolloverInterval(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setMinorCompaction(boolean auto) {
    configHolder.setMinorCompaction(auto);
    return this;
  }
  
  @Override
  public HDFSStoreFactory setMinorCompactionThreads(int count) {
    configHolder.setMinorCompactionThreads(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setMajorCompaction(boolean auto) {
    configHolder.setMajorCompaction(auto);
    return this;
  }

  @Override
  public HDFSStoreFactory setMajorCompactionInterval(int count) {
    configHolder.setMajorCompactionInterval(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setMajorCompactionThreads(int count) {
    configHolder.setMajorCompactionThreads(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setInputFileSizeMax(int size) {
    configHolder.setInputFileSizeMax(size);
    return this;
  }

  @Override
  public HDFSStoreFactory setInputFileCountMin(int count) {
    configHolder.setInputFileCountMin(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setInputFileCountMax(int count) {
    configHolder.setInputFileCountMax(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setPurgeInterval(int interval) {
    configHolder.setPurgeInterval(interval);
    return this;
  }

  @Override
  public HDFSStoreFactory setDiskStoreName(String name) {
    configHolder.setDiskStoreName(name);
    return this;
  }

  @Override
  public HDFSStoreFactory setMaxMemory(int memory) {
    configHolder.setMaxMemory(memory);
    return this;
  }

  @Override
  public HDFSStoreFactory setBatchInterval(int intervalMillis) {
    configHolder.setBatchInterval(intervalMillis);
    return this;
  }

  @Override
  public HDFSStoreFactory setBatchSize(int size) {
    configHolder.setBatchSize(size);
    return this;
  }

  @Override
  public HDFSStoreFactory setBufferPersistent(boolean isPersistent) {
    configHolder.setBufferPersistent(isPersistent);
    return this;
  }

  @Override
  public HDFSStoreFactory setSynchronousDiskWrite(boolean isSynchronous) {
    configHolder.setSynchronousDiskWrite(isSynchronous);
    return this;
  }

  @Override
  public HDFSStoreFactory setDispatcherThreads(int dispatcherThreads) {
    configHolder.setDispatcherThreads(dispatcherThreads);
    return this;
  }
  
  /**
   * This method should not be called on this class.
   * @see HDFSStoreFactory#create(String)
   */
  @Override
  public HDFSStore create(String name) throws GemFireConfigException,
      StoreExistsException {
    throw new UnsupportedOperationException();
  }

  public static void assertIsPositive(String name, int count) {
    if (count < 1) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE
              .toLocalizedString(new Object[] { name, count }));
    }
  }
}
