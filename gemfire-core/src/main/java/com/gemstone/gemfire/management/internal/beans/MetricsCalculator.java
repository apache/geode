/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;


import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.management.internal.ManagementConstants;

/**
 * This is a utility class to calculate various type of Metrics out of raw stats
 * 
 * @author rishim
 * 
 */
public class MetricsCalculator {

  private static TimeUnit milliSeconds = TimeUnit.MILLISECONDS;

  private static long toSeconds(long fromTime, long toTime) {
    return milliSeconds.toSeconds((toTime - fromTime));
  }

  public static float getRate(int Xn, int Xn1, long fromTime, long toTime) {
    float tempXn = Xn;
    float tempXn1 = Xn1;
    long secondsFactor;
    if(fromTime == 0){
      secondsFactor = 0;
    }else{
      secondsFactor = toSeconds(fromTime,toTime);
    }
   
    float num = (tempXn1 - tempXn) / ((secondsFactor == 0) ? 1 : secondsFactor);
       return Round(num, 3);
    // return num;
  }

  public static float getRate(long Xn, long Xn1, long fromTime, long toTime) {
    float tempXn = Xn;
    float tempXn1 = Xn1;
    long secondsFactor;
    if(fromTime == 0){
      secondsFactor = 0;
    }else{
      secondsFactor = toSeconds(fromTime,toTime);
    }
    float num = (tempXn1 - tempXn) /((secondsFactor == 0) ? 1 : secondsFactor);
    return Round(num, 3);
    // return num;

  }

  public static long getLatency(int Xn, int Xn1, long XNTime, long XN1Time) {
    if ((Xn1 - Xn) != 0) {
      return (XN1Time -XNTime) / (Xn1 - Xn);
    }
    return 0;

  }

  public static long getLatency(long Xn, long Xn1, long XNTime, long XN1Time) {
    if ((Xn1 - Xn) != 0) {
      return (XN1Time -XNTime) / (Xn1 - Xn);
    }
    return 0;
  }

  public static long getAverageLatency(int Xn, long XNTime) {
    if (Xn != 0 && XNTime != 0) {
      return XNTime / Xn;
    }
    return 0;
  }

  public static long getAverageLatency(long Xn, long XNTime) {
    if (Xn != 0 && XNTime != 0) {
      return XNTime / Xn;
    }
    return 0;
  }

  // Overloaded getAverage Methods //

  public static int getAverage(int totalNumber, int size) {
    if (totalNumber != 0 && size != 0) {
      return totalNumber / size;
    }
    return 0;
  }

  public static long getAverage(long totalNumber, int size) {
    if (totalNumber != 0 && size != 0) {
      return totalNumber / size;
    }
    return 0;
  }

  public static float getAverage(float totalNumber, int size) {
    if (totalNumber != 0 && size != 0) {

      return totalNumber / size;
    }
    return 0;
  }

  public static double getAverage(double totalNumber, int size) {
    if (totalNumber != 0 && size != 0) {

      return totalNumber / size;
    }
    return 0;
  }

  public static float Round(float Rval, int Rpl) {
    float p = (float) Math.pow(10, Rpl);
    Rval = Rval * p;
    float tmp = Math.round(Rval);
    return (float) tmp / p;
  }
   
  

}
