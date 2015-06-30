/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence.gemfire;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.sequence.LineMapper;

/**
 * A lifeline mapper that just returns a shortened version of 
 * a member id.
 * @author dsmith
 *
 */
public class DefaultLineMapper implements LineMapper {
  private static Pattern MEMBER_ID_RE = Pattern.compile(".*\\((\\d+)(:admin)?(:loner)?\\).*:\\d+(/\\d+|.*:.*)");

  public String getShortNameForLine(String name) {
    Matcher matcher = MEMBER_ID_RE.matcher(name);
    if(matcher.matches()) {
      return matcher.group(1);
    } else {
      return name;
    }
  }

}
