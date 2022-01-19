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
package org.apache.geode.management.internal.beans;


import java.util.concurrent.TimeUnit;

import org.apache.geode.annotations.Immutable;


/**
 * This is a utility class to calculate various type of Metrics out of raw stats
 *
 *
 */
public class MetricsCalculator {

  @Immutable
  private static final TimeUnit milliSeconds = TimeUnit.MILLISECONDS;

  private static long toSeconds(long fromTime, long toTime) {
    return milliSeconds.toSeconds((toTime - fromTime));
  }

  public static float getRate(int Xn, int Xn1, long fromTime, long toTime) {
    float tempXn = Xn;
    float tempXn1 = Xn1;
    long secondsFactor;
    if (fromTime == 0) {
      secondsFactor = 0;
    } else {
      secondsFactor = toSeconds(fromTime, toTime);
    }

    float num = (tempXn1 - tempXn) / ((secondsFactor == 0) ? 1 : secondsFactor);
    return Round(num, 3);
    // return num;
  }

  public static float getRate(long Xn, long Xn1, long fromTime, long toTime) {
    float tempXn = Xn;
    float tempXn1 = Xn1;
    long secondsFactor;
    if (fromTime == 0) {
      secondsFactor = 0;
    } else {
      secondsFactor = toSeconds(fromTime, toTime);
    }
    float num = (tempXn1 - tempXn) / ((secondsFactor == 0) ? 1 : secondsFactor);
    return Round(num, 3);
    // return num;

  }

  public static long getLatency(int Xn, int Xn1, long XNTime, long XN1Time) {
    if ((Xn1 - Xn) != 0) {
      return (XN1Time - XNTime) / (Xn1 - Xn);
    }
    return 0;

  }

  public static long getLatency(long Xn, long Xn1, long XNTime, long XN1Time) {
    if ((Xn1 - Xn) != 0) {
      return (XN1Time - XNTime) / (Xn1 - Xn);
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
    return tmp / p;
  }



}
