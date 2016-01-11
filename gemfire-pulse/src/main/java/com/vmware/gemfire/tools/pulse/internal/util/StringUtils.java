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
 * Class StringUtils This is utility class for string.
 * 
 * @author Sachin K
 * 
 * @since version 7.0.1
 */
public class StringUtils {
  /**
   * Checks the string if it is not null, not empty, and not white space only
   * using standard Java classes.
   * 
   * @param string
   *          String to be checked.
   * @return {@code true} if provided String is not null, is not empty, and has
   *         at least one character that is not considered white space.
   */
  public static boolean isNotNullNotEmptyNotWhiteSpace(final String string) {
    return string != null && !string.isEmpty() && !string.trim().isEmpty();
  }

  /**
   * Checking for String that is not null, not empty, and not white space only
   * using standard Java classes.
   * 
   * @param value
   *          String to be made compliant.
   * @return string compliant string.
   */
  public static String makeCompliantName(String value) {
    value = value.replace(':', '-');
    value = value.replace(',', '-');
    value = value.replace('=', '-');
    value = value.replace('*', '-');
    value = value.replace('?', '-');
    if (value.length() < 1) {
      value = "nothing";
    }
    return value;
  }

  /**
   * Function to get table name derived from region name/full path
   * 
   * @param regionName
   *          String to be made compliant.
   * @return string compliant string.
   */
  public static String getTableNameFromRegionName(String regionName) {
    String tableName = regionName.replaceFirst("/", "").replace('/', '.');
    return tableName;
  }

  /**
   * Function to get region name/full path derived from table name
   * 
   * @param regionName
   *          String to be made compliant.
   * @return string compliant string.
   */
  public static String getRegionNameFromTableName(String tableName) {
    String regionName = "/" + tableName.replace('.', '/');
    return regionName;
  }
}
