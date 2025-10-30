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
package org.apache.geode.management.internal.cli.converters;

import static org.apache.geode.cache.Region.SEPARATOR;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Spring Shell 3.x converter for region paths.
 *
 * <p>
 * Converts a region path string to a normalized region path with proper separator prefix.
 * Used by commands that operate on regions (destroy, alter, describe, etc.).
 *
 * <p>
 * Conversion rules:
 * <ul>
 * <li>Adds {@link org.apache.geode.cache.Region#SEPARATOR} prefix if missing</li>
 * <li>Rejects bare separator "/" as invalid</li>
 * <li>Preserves sub-region paths: "/parent/child"</li>
 * </ul>
 *
 * <p>
 * Example conversions:
 *
 * <pre>
 * "region"           → "/region"
 * "/region"          → "/region"
 * "parent/child"     → "/parent/child"
 * "/parent/child"    → "/parent/child"
 * "/"                → IllegalArgumentException
 * </pre>
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Converter handled both conversion AND region name completion
 * - Spring Shell 3.x: Conversion only; completion via ValueProvider
 * - Conversion logic preserved: adds SEPARATOR prefix, validates input
 * - Completion logic removed: getAllPossibleValues(), getAllRegionPaths()
 *
 * @since GemFire 7.0
 */
@Component
public class RegionPathConverter implements Converter<String, String> {

  /**
   * Converts a region path string to a normalized region path.
   *
   * @param source the region path (with or without leading separator)
   * @return normalized region path with leading separator
   * @throws IllegalArgumentException if source is just the separator "/"
   */
  @Override
  public String convert(@NonNull String source) {
    if (source == null) {
      return null;
    }

    if (source.equals(SEPARATOR)) {
      throw new IllegalArgumentException("invalid region path: " + source);
    }

    if (!source.startsWith(SEPARATOR)) {
      source = SEPARATOR + source;
    }

    return source;
  }
}
