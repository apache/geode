/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.util;

/**
 * Class TimeUtils 
 * 
 * This is utility class used for conversions of time.
 * 
 * @author Sachin K
 * 
 * @since version 7.0.1
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
