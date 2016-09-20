/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.util;

/**
 * Class TimeUtils 
 * 
 * This is utility class used for conversions of time.
 * 
 * 
 * @since GemFire version 7.0.1
 */
public class TimeUtils {

  /**
   * Method to converts time given in milliseconds to string representation in
   * days, hours, minutes and seconds
   * 
   * @param longMilliSecs
   *          Time in milliseconds.
   * @return String
   */
  public static String convertTimeMillisecondsToHMS(long longMilliSecs) {

    long days = longMilliSecs / (1000 * 60 * 60 * 24);
    long remainder = longMilliSecs % (1000 * 60 * 60 * 24);

    long hours = remainder / (1000 * 60 * 60);
    remainder = remainder % (1000 * 60 * 60);

    long mins = remainder / (1000 * 60);
    remainder = remainder % (1000 * 60);

    long secs = remainder / 1000;

    String strDaysHrsMinsSecs = "";

    if (days > 0) {
      strDaysHrsMinsSecs += days + " Days ";
    }

    if (hours > 0) {
      strDaysHrsMinsSecs += hours + " Hours ";
    } else {
      strDaysHrsMinsSecs += "0 Hours ";
    }

    if (mins > 0) {
      strDaysHrsMinsSecs += mins + " Mins ";
    } else {
      strDaysHrsMinsSecs += "0 Mins ";
    }

    strDaysHrsMinsSecs += secs + " Secs";

    return strDaysHrsMinsSecs;
  }

  /**
   * Method to converts time given in seconds to string representation in days,
   * hours, minutes and seconds
   * 
   * @param longSecs
   *          Time in seconds.
   * @return String
   */
  public static String convertTimeSecondsToHMS(long longSecs) {

    long days = longSecs / (60 * 60 * 24);
    long remainder = longSecs % (60 * 60 * 24);

    long hours = remainder / (60 * 60);
    remainder = remainder % (60 * 60);

    long mins = remainder / (60);
    remainder = remainder % (60);

    long secs = remainder;

    String strDaysHrsMinsSecs = "";

    if (days > 0) {
      strDaysHrsMinsSecs += days + " Days ";
    }

    if (hours > 0) {
      strDaysHrsMinsSecs += hours + " Hours ";
    } else {
      strDaysHrsMinsSecs += "0 Hours ";
    }

    if (mins > 0) {
      strDaysHrsMinsSecs += mins + " Mins ";
    } else {
      strDaysHrsMinsSecs += "0 Mins ";
    }

    strDaysHrsMinsSecs += secs + " Secs";

    return strDaysHrsMinsSecs;
  }

}
