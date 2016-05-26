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
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.Statistics;

/**
 * Defines the minimal contract for a StatisticsSampler. This is used by
 * classes that need to interact with the sampler.
 * 
 * @since GemFire 7.0
 */
public interface StatisticsSampler {

  /**
   * Returns the number of times a statistics resource has been add or deleted.
   */
  public int getStatisticsModCount();
  
  /**
   * Returns an array of all the current statistics resource instances.
   */
  public Statistics[] getStatistics();
  
  /**
   * Waits for the SampleCollector to be created and initialized.
   * 
   * @param timeout maximum number of milliseconds to wait
   * @return the initialized SampleCollector or null if timed out
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public SampleCollector waitForSampleCollector(long timeout) throws InterruptedException;
  
  /**
   * Waits for at least one statistics sample to occur before returning.
   * 
   * @param timeout maximum number of milliseconds to wait
   * @return true if a statistics sample occurred; false if wait timed out
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public boolean waitForSample(long timeout) throws InterruptedException;
}
