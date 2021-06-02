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
package org.apache.geode.management.internal.cli.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.management.internal.cli.GfshParser;

public class MultiCommandHelper {

  public static List<String> getMultipleCommands(String input) {
    Map<Integer, List<String>> listMap = new HashMap<Integer, List<String>>();
    String as[] = input.split(GfshParser.COMMAND_DELIMITER);
    int splitCount = 0;
    for (String a : as) {
      if (a.endsWith("\\")) {
        String a2 = a.substring(0, a.length() - 1);
        updateList(listMap, a2, splitCount);
      } else {
        updateList(listMap, a, splitCount);
        splitCount++;
      }
    }
    List<String> finalList = new ArrayList<String>();
    for (int i = 0; i < listMap.size(); i++) {
      List<String> list = listMap.get(i);
      StringBuilder sb = new StringBuilder();
      for (int k = 0; k < list.size(); k++) {
        sb.append(list.get(k));
        if (k < list.size() - 1) {
          sb.append(";");
        }
      }
      finalList.add(sb.toString());
    }
    return finalList;
  }

  private static void updateList(Map<Integer, List<String>> listMap, String a, int splitCount) {
    if (listMap.containsKey(splitCount)) {
      listMap.get(splitCount).add(a);
    } else {
      List<String> list = new ArrayList<String>();
      list.add(a);
      listMap.put(splitCount, list);
    }
  }

}
