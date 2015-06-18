/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;

/**
 * Factory for creating instances of {@link HDFSStore}. To get an instance of
 * this factory call {@link Cache#createHDFSStoreFactory}.
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
   */
  public HDFSStoreFactory setBlockCacheSize(float percentage);

  /**
   * @see HDFSStore#getMaxWriteOnlyFileSize()
   */
  public HDFSStoreFactory setMaxWriteOnlyFileSize(int maxFileSize);

  /**
   * @see HDFSStore#getWriteOnlyFileRolloverInterval()
   */
  public HDFSStoreFactory setWriteOnlyFileRolloverInterval(int interval);

  /**
   * @see HDFSStore#getMinorCompaction()
   */
  public HDFSStoreFactory setMinorCompaction(boolean auto);

  /**
   * @see HDFSStore#getMinorCompactionThreads()
   */
  public HDFSStoreFactory setMinorCompactionThreads(int count);

  /**
   * @see HDFSStore#getMajorCompaction()
   */
  public HDFSStoreFactory setMajorCompaction(boolean auto);

  /**
   * @see HDFSStore#getMajorCompactionInterval()
   */
  public HDFSStoreFactory setMajorCompactionInterval(int interval);

  /**
   * @see HDFSStore#getMajorCompactionThreads()
   */
  public HDFSStoreFactory setMajorCompactionThreads(int count);

  /**
   * @see HDFSStore#getMaxInputFileSizeMB()
   */
  public HDFSStoreFactory setMaxInputFileSizeMB(int size);

  /**
   * @see HDFSStore#getMinInputFileCount()
   */
  public HDFSStoreFactory setMinInputFileCount(int count);

  /**
   * @see HDFSStore#getMaxInputFileCount()
   */
  public HDFSStoreFactory setMaxInputFileCount(int count);

  /**
   * @see HDFSStore#getPurgeInterval()
   */
  public HDFSStoreFactory setPurgeInterval(int interval);

  /**
   * @see HDFSStore#getDiskStoreName()
   */
  public HDFSStoreFactory setDiskStoreName(String name);

  /**
   * @see HDFSStore#getMaxMemory()
   */
  public HDFSStoreFactory setMaxMemory(int memory);

  /**
   * @see HDFSStore#getBatchInterval()
   */
  public HDFSStoreFactory setBatchInterval(int interval);

  /**
   * @see HDFSStore#getBatchSize()
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
