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
 * 
 * @since GemFire version 7.0.1
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
