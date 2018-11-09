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

package org.apache.geode.redis.internal;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HashNeighbors {
  public String center;
  public String west;
  public String east;
  public String north;
  public String south;
  public String northwest;
  public String northeast;
  public String southwest;
  public String southeast;

  public List<String> get() {
    return Arrays
        .asList(center, west, east, north, south, northwest, northeast, southwest, southeast)
        .stream()
        .filter(n -> n != null)
        .distinct()
        .collect(Collectors.toList());
  }
}
