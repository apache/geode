/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

/**
 * HDFSStoreMutator provides a means to dynamically alter {@link HDFSStore}'s
 * behavior. Instances of this interface are created using
 * {@link HDFSStore#createHdfsStoreMutator} and applied using
 * {@link HDFSStore#alter}
 * 
 * @author ashvina
 */
public interface HDFSStoreMutator {
  /**
   * {@link HDFSStoreFactory#setWriteOnlyFileSizeLimit(int)}
   */
  public HDFSStoreMutator setWriteOnlyFileSizeLimit(int maxFileSize);

  /**
   * {@link HDFSStore#getWriteOnlyFileSizeLimit()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getWriteOnlyFileSizeLimit();

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
   * {@link HDFSStoreFactory#setInputFileSizeMax(int)}
   */
  public HDFSStoreMutator setInputFileSizeMax(int size);

  /**
   * {@link HDFSStore#getInputFileSizeMax()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getInputFileSizeMax();

  /**
   * {@link HDFSStoreFactory#setInputFileCountMin(int)}
   */
  public HDFSStoreMutator setInputFileCountMin(int count);

  /**
   * {@link HDFSStore#getInputFileCountMin()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getInputFileCountMin();

  /**
   * {@link HDFSStoreFactory#setInputFileCountMax(int)}
   */
  public HDFSStoreMutator setInputFileCountMax(int count);

  /**
   * {@link HDFSStore#getInputFileCountMax()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getInputFileCountMax();

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
