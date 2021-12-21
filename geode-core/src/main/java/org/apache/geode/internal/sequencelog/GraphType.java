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
package org.apache.geode.internal.sequencelog;

import java.util.EnumSet;

public enum GraphType {
  REGION, KEY, MESSAGE, MEMBER;

  public byte getId() {
    return (byte) ordinal();
  }

  public static GraphType getType(byte id) {
    return values()[id];
  }

  public static EnumSet<GraphType> parse(String enabledTypesString) {

    EnumSet<GraphType> set = EnumSet.noneOf(GraphType.class);
    if (enabledTypesString.contains("region")) {
      set.add(REGION);
    }
    if (enabledTypesString.contains("key")) {
      set.add(KEY);
    }
    if (enabledTypesString.contains("message")) {
      set.add(MESSAGE);
    }
    if (enabledTypesString.contains("member")) {
      set.add(MEMBER);
    }
    if (enabledTypesString.contains("all")) {
      set = EnumSet.allOf(GraphType.class);
    }

    return set;
  }
}
