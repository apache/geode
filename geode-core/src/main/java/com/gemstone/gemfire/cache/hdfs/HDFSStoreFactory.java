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

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;

/**
 * Factory for creating instances of {@link HDFSStore}. To get an instance of
 * this factory call Cache#createHDFSStoreFactory.
 * <P>
 * Usage
 * <ol>
 * <li> configure factory using <code>set</code> methods
 * <li> call {@link #create} to produce a HDFSStore instance.
 * </ol>
 * 
 * @author Hemant Bhanawat
 * @author Ashvin Agrawal
 */
public interface HDFSStoreFactory {

  /**
   * @see HDFSStore#getName()
   */
  public HDFSStoreFactory setName(String name);

  /**
   * @see HDFSStore#getNameNodeURL()
   */
  public HDFSStoreFactory setNameNodeURL(String url);

  /**
   * @see HDFSStore#getHomeDir()
   */
  public HDFSStoreFactory setHomeDir(String dir);

  /**
   * @see HDFSStore#getHDFSClientConfigFile()
   */
  public HDFSStoreFactory setHDFSClientConfigFile(String filePath);

  /**
   * @see HDFSStore#getHDFSClientConfigFile()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 or more than 100
   */
  public HDFSStoreFactory setBlockCacheSize(float value);

  /**
   * Default value {@link HDFSStore#DEFAULT_WRITE_ONLY_FILE_SIZE_LIMIT}
   * @see HDFSStore#getWriteOnlyFileRolloverSize()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setWriteOnlyFileRolloverSize(int maxFileSize);

  /**
   * Default value {@link HDFSStore#DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL}
   * @see HDFSStore#getWriteOnlyFileRolloverInterval()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setWriteOnlyFileRolloverInterval(int interval);

  /**
   * Default value {@link HDFSStore#DEFAULT_MINOR_COMPACTION}
   * @see HDFSStore#getMinorCompaction()
   */
  public HDFSStoreFactory setMinorCompaction(boolean auto);

  /**
   * Default value {@link HDFSStore#DEFAULT_MINOR_COMPACTION_THREADS}
   * @see HDFSStore#getMinorCompactionThreads()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setMinorCompactionThreads(int count);

  /**
   * Default value {@link HDFSStore#DEFAULT_MAJOR_COMPACTION}
   * @see HDFSStore#getMajorCompaction()
   */
  public HDFSStoreFactory setMajorCompaction(boolean auto);

  /**
   * Default value {@link HDFSStore#DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS}
   * @see HDFSStore#getMajorCompactionInterval()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setMajorCompactionInterval(int interval);

  /**
   * Default value {@link HDFSStore#DEFAULT_MAJOR_COMPACTION_THREADS}
   * @see HDFSStore#getMajorCompactionThreads()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setMajorCompactionThreads(int count);

  /**
   * Default value {@link HDFSStore#DEFAULT_INPUT_FILE_SIZE_MAX_MB}
   * @see HDFSStore#getInputFileSizeMax()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setInputFileSizeMax(int size);

  /**
   * Default value {@link HDFSStore#DEFAULT_INPUT_FILE_COUNT_MIN}
   * @see HDFSStore#getInputFileCountMin()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setInputFileCountMin(int count);

  /**
   * Default value {@link HDFSStore#DEFAULT_INPUT_FILE_COUNT_MAX}
   * @see HDFSStore#getInputFileCountMax()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setInputFileCountMax(int count);

  /**
   * @see HDFSStore#getPurgeInterval()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setPurgeInterval(int interval);

  /**
   * @see HDFSStore#getDiskStoreName()
   */
  public HDFSStoreFactory setDiskStoreName(String name);

  /**
   * @see HDFSStore#getMaxMemory()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setMaxMemory(int memory);

  /**
   * @see HDFSStore#getBatchInterval()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setBatchInterval(int interval);

  /**
   * @see HDFSStore#getBatchSize()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setBatchSize(int size);

  /**
   * @see HDFSStore#getBufferPersistent()
   */
  public HDFSStoreFactory setBufferPersistent(boolean isPersistent);

  /**
   * @see HDFSStore#getSynchronousDiskWrite()
   */
  public HDFSStoreFactory setSynchronousDiskWrite(boolean isSynchronous);

  /**
   * @see HDFSStore#getDispatcherThreads()
   * @exception IllegalArgumentException
   *              if the {@code value} is less than 0 
   */
  public HDFSStoreFactory setDispatcherThreads(int dispatcherThreads);

  /**
   * Validates all attribute values and assigns defaults where applicable.
   * Creates a new instance of {@link HDFSStore} based on the current attribute
   * values configured in this factory.
   * 
   * @param name
   *          the name of the HDFSStore
   * @return the newly created HDFSStore.
   * @throws GemFireConfigException
   *           if the configuration is invalid
   * @throws StoreExistsException
   *           if a {@link HDFSStore} with the same name exists
   */
  public HDFSStore create(String name) throws GemFireConfigException, StoreExistsException;
}
