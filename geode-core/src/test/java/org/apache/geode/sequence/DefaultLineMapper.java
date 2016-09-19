/*
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
 */
package org.apache.geode.sequence;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.sequence.LineMapper;

/**
 * A lifeline mapper that just returns a shortened version of 
 * a member id.
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
