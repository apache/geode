/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

public interface HDFSStoreMutator {
  /**
   * {@link HDFSStoreFactory#setMaxWriteOnlyFileSize(int)}
   */
  public HDFSStoreMutator setMaxWriteOnlyFileSize(int maxFileSize);

  /**
   * {@link HDFSStore#getMaxWriteOnlyFileSize()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMaxWriteOnlyFileSize();

  /**
   * {@link HDFSStoreFactory#setWriteOnlyFileRolloverInterval(int)}
   */
  public HDFSStoreMutator setWriteOnlyFileRolloverInterval(int interval);

  /**
   * {@link HDFSStore#getWriteOnlyFileRolloverInterval()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getWriteOnlyFileRolloverInterval();

  /**
   * {@link HDFSStore#getMinorCompaction()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. null if not
   *         set
   */
  public Boolean getMinorCompaction();

  /**
   * {@link HDFSStoreFactory#setMinorCompaction(boolean)}
   */
  public HDFSStoreMutator setMinorCompaction(boolean auto);

  /**
   * {@link HDFSStoreFactory#setMinorCompactionThreads(int)}
   */
  public HDFSStoreMutator setMinorCompactionThreads(int count);

  /**
   * {@link HDFSStore#getMinorCompactionThreads()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMinorCompactionThreads();

  /**
   * {@link HDFSStoreFactory#setMajorCompaction(boolean)}
   */
  public HDFSStoreMutator setMajorCompaction(boolean auto);

  /**
   * {@link HDFSStore#getMajorCompaction()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. null if not
   *         set
   */
  public Boolean getMajorCompaction();

  /**
   * {@link HDFSStoreFactory#setMajorCompactionInterval(int)}
   */
  public HDFSStoreMutator setMajorCompactionInterval(int interval);

  /**
   * {@link HDFSStore#getMajorCompactionInterval()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMajorCompactionInterval();

  /**
   * {@link HDFSStoreFactory#setMajorCompactionThreads(int)}
   */
  public HDFSStoreMutator setMajorCompactionThreads(int count);

  /**
   * {@link HDFSStore#getMajorCompactionThreads()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMajorCompactionThreads();

  /**
   * {@link HDFSStoreFactory#setMaxInputFileSizeMB(int)}
   */
  public HDFSStoreMutator setMaxInputFileSizeMB(int size);

  /**
   * {@link HDFSStore#getMaxInputFileSizeMB()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMaxInputFileSizeMB();

  /**
   * {@link HDFSStoreFactory#setMinInputFileCount(int)}
   */
  public HDFSStoreMutator setMinInputFileCount(int count);

  /**
   * {@link HDFSStore#getMinInputFileCount()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMinInputFileCount();

  /**
   * {@link HDFSStoreFactory#setMaxInputFileCount(int)}
   */
  public HDFSStoreMutator setMaxInputFileCount(int count);

  /**
   * {@link HDFSStore#getMaxInputFileCount()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMaxInputFileCount();

  /**
   * {@link HDFSStoreFactory#setPurgeInterval(int)}
   */
  public HDFSStoreMutator setPurgeInterval(int interval);

  /**
   * {@link HDFSStore#getPurgeInterval()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getPurgeInterval();

  /**
   * {@link HDFSStore#getBatchSize()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getBatchSize();

  /**
   * {@link HDFSStoreFactory#setBatchSize(int)}
   */
  public HDFSStoreMutator setBatchSize(int size);

  /**
   * {@link HDFSStore#getBatchInterval()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getBatchInterval();

  /**
   * {@link HDFSStoreFactory#setBatchInterval(int)}
   */
  public HDFSStoreMutator setBatchInterval(int interval);
}
