/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


/**
 * Defines the common date format for GemFire and provides DateFormat instances.
 */
public class DateFormatter {

  /**
   * The format string used to format the timestamp of GemFire log messages
   */
  public static final String FORMAT_STRING = "yyyy/MM/dd HH:mm:ss.SSS z";

  /**
   * the format string used to format the date/time when localized pattern is not ideal
   */
  public static final String LOCALIZED_FORMAT_STRING = "EEE yyyy/MM/dd HH:mm:ss zzz";

  /**
   * Creates a SimpleDateFormat using {@link #FORMAT_STRING}.
   *
   * Thread Safety Issue: (From SimpleDateFormat) Date formats are not synchronized. It is
   * recommended to create separate format instances for each thread. If multiple threads access a
   * format concurrently, it must be synchronized externally.
   */
  public static DateFormat createDateFormat() {
    return new SimpleDateFormat(FORMAT_STRING);
  }

  public static SimpleDateFormat createLocalizedDateFormat() {
    String pattern = new SimpleDateFormat().toLocalizedPattern();
    return new SimpleDateFormat(getModifiedLocalizedPattern(pattern), Locale.getDefault());
  }

  static String getModifiedLocalizedPattern(String pattern) {
    String pattern_to_use;
    // will always try to use the localized pattern if the pattern displays the minutes
    if (pattern.contains("mm")) {
      // if the localized pattern does not display seconds, add it in the pattern
      if (!pattern.contains("ss")) {
        int mm = pattern.indexOf("mm");
        pattern_to_use = pattern.substring(0, mm + 2) + ":ss" + pattern.substring(mm + 2);
      }
      // if the localized pattern already contains seconds, the pattern should be enough
      else {
        pattern_to_use = pattern;
      }

      // add the days of week to the pattern if not there yet
      if (!pattern_to_use.contains("EEE")) {
        pattern_to_use = "EEE " + pattern_to_use;
      }

      if (!pattern_to_use.contains("zzz")) {
        pattern_to_use = pattern_to_use + " zzz";
      }
    }
    // if the localized pattern doesn't even contain the minutes, then giving up using the
    // localized pattern since we have no idea what it is
    else {
      pattern_to_use = LOCALIZED_FORMAT_STRING;
    }

    return pattern_to_use;
  }

  /**
   * Creates a SimpleDateFormat using specified formatString.
   */
  public static DateFormat createDateFormat(final String formatString) {
    return new SimpleDateFormat(formatString);
  }

  /**
   * Gets a String representation of the current time.
   *
   * @return a String representation of the current time.
   */
  public static String getTimeStamp() {
    return formatDate(new Date());
  }

  /**
   * Convert a Date to a timestamp String.
   *
   * @param d a Date to format as a timestamp String.
   * @return a String representation of the current time.
   */
  public static String formatDate(final Date d) {
    try {
      return createDateFormat().format(d);
    } catch (Exception e1) {
      // Fix bug 21857
      try {
        return d.toString();
      } catch (Exception e2) {
        try {
          return Long.toString(d.getTime());
        } catch (Exception e3) {
          return "timestampFormatFailed";
        }
      }
    }
  }

  /**
   * Do not instantiate this class.
   */
  private DateFormatter() {
    // do not instantiate this class
  }
}
