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

/**
 * The configuration of a {@link org.apache.geode.pdx.ReflectionBasedAutoSerializer}
 * that is used for easy PDX serialization.
 */
public class AutoSerializer implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Boolean portable;
  private final List<String> patterns;

  /**
   * Creates a AutoSerializer that is not portable.
   */
  public AutoSerializer(String... patterns) {
    this(false, patterns);
  }

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
