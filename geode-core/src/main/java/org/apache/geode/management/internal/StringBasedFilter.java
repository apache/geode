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
package org.apache.geode.management.internal;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.management.ManagementException;

/**
 * This is a string pattern based Filter with some limitations.
 *
 *
 */

public class StringBasedFilter {

  private final FilterParam params;

  private final List<Pattern> exclusionPatternList;
  private final List<Pattern> inclusionPatternList;

  public StringBasedFilter(FilterParam params) {
    this.params = params;
    exclusionPatternList = new ArrayList<>();
    inclusionPatternList = new ArrayList<>();

    compileFilterList(params.getExclusionList(), exclusionPatternList);
    compileFilterList(params.getInclusionList(), inclusionPatternList);

  }

  public boolean isExcluded(String tokenToMatch) {
    if (params.isDefaultExcludeFilter()) {
      return false;
    }
    tokenToMatch = formatStringTokens(tokenToMatch);
    for (Pattern pattern : exclusionPatternList) {
      Matcher matcher = pattern.matcher(tokenToMatch);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }


  public boolean isIncluded(String tokenToMatch) {
    if (params.isDefaultIncludeFilter()) {
      return true;
    }
    tokenToMatch = formatStringTokens(tokenToMatch);
    for (Pattern pattern : inclusionPatternList) {
      Matcher matcher = pattern.matcher(tokenToMatch);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }

  private void compileFilterList(String[] list, List<Pattern> patternList) {


    for (String s : list) {
      try {
        s = formatStringTokens(s);
        Pattern pattern;
        if (s.contains("*")) {
          pattern = Pattern.compile(s);
        } else {
          pattern = Pattern.compile(s, Pattern.LITERAL);
        }

        patternList.add(pattern);
      } catch (NullPointerException e) {
        throw new ManagementException(e);
      }
    }
  }


  private String formatStringTokens(String value) {

    value = value.replace('<', '-');
    value = value.replace('>', '-');
    value = value.replace('(', '-');
    value = value.replace(')', '-');

    if (value.length() < 1) {
      value = "nothing";
    }
    return value;
  }


}
