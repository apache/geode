/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;



public class HistogramStats {
  /** stat type description */
  private final static String hist_typeDesc = "A bucketed histogram of values with unit ";
  
  private final int statCounterIndex[];

  private final long bp[];
  
  private final Statistics stats;  
  
  /**
   * Create a set of statistics to capture a histogram of values with the given break points.
   * @param name a unique name for the histogram type
   * @param unit the unit of data collected
   * @param factory
   * @param breakPoints the breakpoints for each bucket
   * @param largerIsBetter
   */
  public HistogramStats(String name, String unit, StatisticsFactory factory, long[] breakPoints, boolean largerIsBetter) {
    this.bp = breakPoints;
    StatisticDescriptor[] fieldDescriptors = new StatisticDescriptor[this.bp.length*2];
    int k = 0;
    for (int bucketNumber = 0; bucketNumber < this.bp.length; bucketNumber++) {
      String desc = (bucketNumber < this.bp.length-1 ? "ForLTE" : "ForGT") + this.bp[bucketNumber]; 
      fieldDescriptors[k] = factory.createIntCounter("BucketCount" + desc,  
          "Number of data points in Bucket " + bucketNumber, "count", !largerIsBetter);
      k++;
      fieldDescriptors[k] = factory.createLongCounter("BucketTotal" + desc,
          "Sum of Bucket " + bucketNumber, unit, !largerIsBetter);
      k++;
    }
    StatisticsType hist_type = factory.createType("HistogramWith" + breakPoints.length + "Buckets",
        hist_typeDesc + unit + " for " + breakPoints.length + " breakpoints", fieldDescriptors);
    this.statCounterIndex = new int[this.bp.length*2];
    k=0;
    for (int bucketNumber = 0; bucketNumber < this.bp.length; bucketNumber++) {
      String desc = (bucketNumber < this.bp.length-1 ? "ForLTE" : "ForGT") + this.bp[bucketNumber]; 
      this.statCounterIndex[k] = hist_type.nameToId("BucketCount" + desc);
      k++;
      this.statCounterIndex[k] = hist_type.nameToId("BucketTotal" + desc);
      k++;
    }
    this.stats = factory.createAtomicStatistics(hist_type, name, 0L);
  }

  public void endOp(long delta) {
    int index = this.statCounterIndex.length - 2;
    for (int i=0; i<this.bp.length; i++) {
      if (delta <= this.bp[i]) {
        index = i * 2;
        break;
      }
    }
    this.stats.incInt(this.statCounterIndex[index], 1);
    this.stats.incLong(this.statCounterIndex[index+1], delta);
  }
}
