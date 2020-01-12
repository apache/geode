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
 *
 */

package org.apache.geode.management.configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.management.api.ClusterManagementService;

/**
 * The configuration of a org.apache.geode.pdx.ReflectionBasedAutoSerializer
 * found in the geode-core module that is used for easy PDX serialization.
 * At least one pattern is required if the AutoSerializer is used for a
 * {@link ClusterManagementService#create}.
 */
public class AutoSerializer implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Boolean portable;
  private final List<String> patterns;

  /**
   * Creates an AutoSerializer that is not portable.
   *
   * @param patterns regular expressions that will cause the auto serializer to serialize any
   *        classes whose fully qualified class name matches one of the patterns.
   *        If no patterns are given, and the operation is CREATE, then an exception will be thrown.
   */
  public AutoSerializer(String... patterns) {
    this(false, patterns);
  }

  /**
   * Creates an AutoSerializer.
   *
   * @param portable if true then any attempt to serialize a class that is not supported by the
   *        native client will fail.
   * @param patterns regular expressions that will cause the auto serializer to serialize any
   *        classes whose fully qualified class name matches one of the patterns.
   *        If no patterns are given, and the operation is CREATE, then an exception will be thrown.
   */
  public AutoSerializer(boolean portable, String... patterns) {
    this(portable, buildPatternList(patterns));
  }

  private static List<String> buildPatternList(String[] patterns) {
    if (patterns != null && patterns.length > 0) {
      return Arrays.asList(patterns);
    } else {
      return null;
    }
  }

  /**
   * Creates an AutoSerializer.
   *
   * @param portable if true then any attempt to serialize a class that is not supported by the
   *        native client will fail.
   * @param patterns regular expressions that will cause the auto serializer to serialize any
   *        classes whose fully qualified class name matches one of the patterns.
   *        If no patterns are given, and the operation is CREATE, then an exception will be thrown.
   */
  @JsonCreator
  public AutoSerializer(@JsonProperty("portable") Boolean portable,
      @JsonProperty("patterns") List<String> patterns) {
    this.portable = portable;
    this.patterns = patterns;
  }

  public Boolean isPortable() {
    return portable;
  }

  public List<String> getPatterns() {
    return patterns;
  }
}
