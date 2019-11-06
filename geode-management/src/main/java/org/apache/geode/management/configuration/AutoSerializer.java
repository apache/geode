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

/**
 * The configuration of a ReflectionBasedAutoSerializer
 * that is used for easy PDX serialization.
 */
public class AutoSerializer implements Serializable {
  private static final long serialVersionUID = 1L;

  private Boolean portable;
  private List<String> patterns;

  public AutoSerializer(boolean portable, String... patterns) {
    this.portable = portable;
    this.patterns = Arrays.asList(patterns);
  }

  public AutoSerializer(Boolean portable, List<String> pattterns) {
    this.portable = portable;
    this.patterns = pattterns;
  }

  public Boolean isPortable() {
    return portable;
  }

  public void setPortable(Boolean portable) {
    this.portable = portable;
  }

  public List<String> getPatterns() {
    return patterns;
  }

  public void setPatterns(List<String> patterns) {
    this.patterns = patterns;
  }
}
