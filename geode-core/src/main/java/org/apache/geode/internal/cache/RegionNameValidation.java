/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.cache.Region;

class RegionNameValidation {

  private static final Pattern NAME_PATTERN = Pattern.compile("[aA-zZ0-9-_.]+");

  static Pattern getNamePattern() {
    return NAME_PATTERN;
  }

  static void validate(String name) {
    validate(name, new InternalRegionArguments());
  }

  static void validate(String name, InternalRegionArguments internalRegionArguments) {
    if (name == null) {
      throw new IllegalArgumentException(
          "name cannot be null");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
          "name cannot be empty");
    }
    if (name.contains(Region.SEPARATOR)) {
      throw new IllegalArgumentException(
          String.format("name cannot contain the separator ' %s '",
              Region.SEPARATOR));
    }

    // Validate the name of the region only if it isn't an internal region
    if (internalRegionArguments.isInternalRegion()) {
      return;
    }
    if (internalRegionArguments.isUsedForMetaRegion()) {
      return;
    }
    if (internalRegionArguments.isUsedForPartitionedRegionAdmin()) {
      return;
    }
    if (internalRegionArguments.isUsedForPartitionedRegionBucket()) {
      return;
    }

    if (name.startsWith("__")) {
      throw new IllegalArgumentException(
          "Region names may not begin with a double-underscore: " + name);
    }

    // Ensure the region only contains valid characters
    Matcher matcher = NAME_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Region names may only be alphanumeric and may contain hyphens or underscores: " + name);
    }
  }
}
