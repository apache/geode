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
   * {@link HDFSStoreFactory#setWriteOnlyFileRolloverSize(int)}
   */
  public HDFSStoreMutator setWriteOnlyFileRolloverSize(int maxFileSize);

  /**
   * {@link HDFSStore#getWriteOnlyFileRolloverSize()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getWriteOnlyFileRolloverSize();

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
