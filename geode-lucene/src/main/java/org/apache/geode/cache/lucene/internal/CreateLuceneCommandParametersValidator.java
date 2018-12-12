/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CreateLuceneCommandParametersValidator {
  public static void validateRegionName(String name) {
    validateNameNotEmptyOrNull(name);
    String msg =
        "Region names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens, underscores, or forward slashes: ";
    Matcher matcher = Pattern.compile("[aA-zZ0-9-_./]+").matcher(name);
    if (name.startsWith("__") || name.startsWith("/__") || !matcher.matches()) {
      throw new IllegalArgumentException(msg + name);
    }
  }

  public static void validateLuceneIndexName(String name) {
    validateNameNotEmptyOrNull(name);
    String msg =
        "Index names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens or underscores: ";
    Matcher matcher = Pattern.compile("[aA-zZ0-9-_.]+").matcher(name);
    if (name.startsWith("__") || !matcher.matches()) {
      throw new IllegalArgumentException(msg + name);
    }
  }

  private static void validateNameNotEmptyOrNull(String nameProvided) {
    if (nameProvided == null) {
      throw new IllegalArgumentException(
          "name cannot be null");
    }
    if (nameProvided.isEmpty()) {
      throw new IllegalArgumentException(
          "name cannot be empty");
    }
  }
}
