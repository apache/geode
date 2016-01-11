/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/* [ NOTE: 
 * This class supposed to be removed, if required, after discussing with 
 * VMware team ]
 */
/**
 * Class IPAddressUtil This is utility class for checking whether ip address is
 * versions i.e. IPv4 or IPv6 address
 * 
 * @author Sachin K
 * 
 * @since version 7.0.1
 */
public class IPAddressUtil {

  private static Pattern VALID_IPV4_PATTERN = null;
  private static Pattern VALID_IPV6_PATTERN = null;
  private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
  private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

  static {
    try {
      VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern,
          Pattern.CASE_INSENSITIVE);
      VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern,
          Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {

    }
  }

  public static Boolean isIPv4LiteralAddress(String IPAddress) {
    Matcher matcher = IPAddressUtil.VALID_IPV4_PATTERN.matcher(IPAddress);
    return matcher.matches();
  }

  public static Boolean isIPv6LiteralAddress(String IPAddress) {

    Matcher matcher = IPAddressUtil.VALID_IPV6_PATTERN.matcher(IPAddress);
    return matcher.matches();
  }
}
