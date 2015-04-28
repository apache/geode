/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.util.spring;

/**
 * Replaces org.springframework.shell.support.util.StringUtils which
 * is now removed from SPring Shell & the same class is referred from Spring
 * Core. With this we can avoid GemFire member's runtime dependency on Spring
 * Core.
 */
/*
 * Code selectively taken from the original org.springframework.shell.support.util.StringUtils
 */
public class StringUtils {

  /**
   * <p>
   * Removes leading and trailing whitespace from both ends of this String returning an empty String ("") if the String is empty after the trim or if it is <code>null</code>.
   *
   * <pre>
   * StringUtils.trimToNull(null) = ""
   * StringUtils.trimToNull("") = ""
   * StringUtils.trimToNull(" ") = ""
   * StringUtils.trimToNull("abc") = "abc"
   * StringUtils.trimToNull(" abc ") = "abc"
   * </pre>
   *
   * @param str the String to be trimmed, may be null
   * @return the trimmed String, an empty String("") if only chars &lt;= 32, empty or null String input
   * @since 1.1
   */
  public static String trimToEmpty(final String str) {
    String ts = trimWhitespace(str);
    return !hasText(ts) ? "" : ts;
  }

  /**
   * Trim leading and trailing whitespace from the given String.
   * @param str the String to check
   * @return the trimmed String
   * @see java.lang.Character#isWhitespace
   */
  public static String trimWhitespace(final String str) {
    if (!hasLength(str)) {
      return str;
    }
    StringBuilder sb = new StringBuilder(str);
    while (sb.length() > 0 && Character.isWhitespace(sb.charAt(0))) {
      sb.deleteCharAt(0);
    }
    while (sb.length() > 0 && Character.isWhitespace(sb.charAt(sb.length() - 1))) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }
  //---------------------------------------------------------------------
  // General convenience methods for working with Strings
  //---------------------------------------------------------------------

  /**
   * Check that the given CharSequence is neither <code>null</code> nor of length 0.
   * Note: Will return <code>true</code> for a CharSequence that purely consists of whitespace.
   * <p><pre>
   * StringUtils.hasLength(null) = false
   * StringUtils.hasLength("") = false
   * StringUtils.hasLength(" ") = true
   * StringUtils.hasLength("Hello") = true
   * </pre>
   * @param str the CharSequence to check (may be <code>null</code>)
   * @return <code>true</code> if the CharSequence is not null and has length
   * @see #hasText(String)
   */
  public static boolean hasLength(final CharSequence str) {
    return (str != null && str.length() > 0);
  }

  /**
   * Check that the given String is neither <code>null</code> nor of length 0.
   * Note: Will return <code>true</code> for a String that purely consists of whitespace.
   * @param str the String to check (may be <code>null</code>)
   * @return <code>true</code> if the String is not null and has length
   * @see #hasLength(CharSequence)
   */
  public static boolean hasLength(final String str) {
    return hasLength((CharSequence) str);
  }

  /**
   * Check whether the given CharSequence has actual text.
   * More specifically, returns <code>true</code> if the string not <code>null</code>,
   * its length is greater than 0, and it contains at least one non-whitespace character.
   * <p><pre>
   * StringUtils.hasText(null) = false
   * StringUtils.hasText("") = false
   * StringUtils.hasText(" ") = false
   * StringUtils.hasText("12345") = true
   * StringUtils.hasText(" 12345 ") = true
   * </pre>
   * @param str the CharSequence to check (may be <code>null</code>)
   * @return <code>true</code> if the CharSequence is not <code>null</code>,
   * its length is greater than 0, and it does not contain whitespace only
   * @see java.lang.Character#isWhitespace
   */
  public static boolean hasText(final CharSequence str) {
    if (!hasLength(str)) {
      return false;
    }
    int strLen = str.length();
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whether the given String has actual text.
   * More specifically, returns <code>true</code> if the string not <code>null</code>,
   * its length is greater than 0, and it contains at least one non-whitespace character.
   * @param str the String to check (may be <code>null</code>)
   * @return <code>true</code> if the String is not <code>null</code>, its length is
   * greater than 0, and it does not contain whitespace only
   * @see #hasText(CharSequence)
   */
  public static boolean hasText(final String str) {
    return hasText((CharSequence) str);
  }
  
  /**
   * Removes the given prefix from the given string, if it exists
   * 
   * @param str the string to modify (can be blank to do nothing)
   * @param prefix the prefix to remove (can be blank to do nothing)
   * @return <code>null</code> if a <code>null</code> string was given
   * @since 1.2.0
   */
  public static String removePrefix(final String str, final String prefix) {
    if (!hasText(str) || !hasText(prefix) || !str.startsWith(prefix)) {
      return str;
    }
    return str.substring(prefix.length());
  }
  
  /**
   * Removes the given suffix from the given string, if it exists
   * 
   * @param str the string to modify (can be blank to do nothing)
   * @param suffix the suffix to remove (can be blank to do nothing)
   * @return <code>null</code> if a <code>null</code> string was given
   * @since 1.2.0
   */
  public static String removeSuffix(final String str, final String suffix) {
    if (!hasText(str) || !hasText(suffix) || !str.endsWith(suffix)) {
      return str;
    }
    return str.substring(0, str.length() - suffix.length());
  }

  /**
   * Trim trailing whitespace from the given String.
   * @param str the String to check
   * @return the trimmed String
   * @see java.lang.Character#isWhitespace
   */
  public static String trimTrailingWhitespace(final String str) {
    if (!hasLength(str)) {
      return str;
    }
    StringBuilder sb = new StringBuilder(str);
    while (sb.length() > 0 && Character.isWhitespace(sb.charAt(sb.length() - 1))) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * Test if the given String ends with the specified suffix,
   * ignoring upper/lower case.
   * @param str the String to check
   * @param suffix the suffix to look for
   * @see java.lang.String#endsWith
   */
  public static boolean endsWithIgnoreCase(final String str, final String suffix) {
    if (str == null || suffix == null) {
      return false;
    }
    if (str.endsWith(suffix)) {
      return true;
    }
    if (str.length() < suffix.length()) {
      return false;
    }

    String lcStr = str.substring(str.length() - suffix.length()).toLowerCase();
    String lcSuffix = suffix.toLowerCase();
    return lcStr.equals(lcSuffix);
  }

}
