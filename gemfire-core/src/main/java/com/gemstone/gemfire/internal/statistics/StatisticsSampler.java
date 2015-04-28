/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.Statistics;

/**
 * Defines the minimal contract for a StatisticsSampler. This is used by
 * classes that need to interact with the sampler.
 * 
 * @author Kirk Lund
 * @since 7.0
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
