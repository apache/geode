/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;



public class HistogramStats {
  /** stat type description */
  private final static String hist_typeDesc = "A bucketed histogram of values with unit ";

  private final int statCounterIndex[];

  private final long bp[];

  private final Statistics stats;

  /**
   * Create a set of statistics to capture a histogram of values with the given break points.
   * 
   * @param name a unique name for the histogram type
   * @param unit the unit of data collected
   * @param factory
   * @param breakPoints the breakpoints for each bucket
   * @param largerIsBetter
   */
  public HistogramStats(String name, String unit, StatisticsFactory factory, long[] breakPoints,
      boolean largerIsBetter) {
    this.bp = breakPoints;
    StatisticDescriptor[] fieldDescriptors = new StatisticDescriptor[this.bp.length * 2];
    int k = 0;
    for (int bucketNumber = 0; bucketNumber < this.bp.length; bucketNumber++) {
      String desc =
          (bucketNumber < this.bp.length - 1 ? "ForLTE" : "ForGT") + this.bp[bucketNumber];
      fieldDescriptors[k] = factory.createIntCounter("BucketCount" + desc,
          "Number of data points in Bucket " + bucketNumber, "count", !largerIsBetter);
      k++;
      fieldDescriptors[k] = factory.createLongCounter("BucketTotal" + desc,
          "Sum of Bucket " + bucketNumber, unit, !largerIsBetter);
      k++;
    }
    StatisticsType hist_type = factory.createType("HistogramWith" + breakPoints.length + "Buckets",
        hist_typeDesc + unit + " for " + breakPoints.length + " breakpoints", fieldDescriptors);
    this.statCounterIndex = new int[this.bp.length * 2];
    k = 0;
    for (int bucketNumber = 0; bucketNumber < this.bp.length; bucketNumber++) {
      String desc =
          (bucketNumber < this.bp.length - 1 ? "ForLTE" : "ForGT") + this.bp[bucketNumber];
      this.statCounterIndex[k] = hist_type.nameToId("BucketCount" + desc);
      k++;
      this.statCounterIndex[k] = hist_type.nameToId("BucketTotal" + desc);
      k++;
    }
    this.stats = factory.createAtomicStatistics(hist_type, name, 0L);
  }

  public void endOp(long delta) {
    int index = this.statCounterIndex.length - 2;
    for (int i = 0; i < this.bp.length; i++) {
      if (delta <= this.bp[i]) {
        index = i * 2;
        break;
      }
    }
    this.stats.incInt(this.statCounterIndex[index], 1);
    this.stats.incLong(this.statCounterIndex[index + 1], delta);
  }
}
