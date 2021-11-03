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
package org.apache.geode.internal.version;

import java.util.Map;

import org.jetbrains.annotations.NotNull;

/**
 * This interface is used to provide aggregate version information about components of a
 * distribution. A default, and exported implementation, will report the Apache Geode open source
 * version information that we currently produce today. Any additional exported implementations will
 * be reported as well. The order in which the components are reported is undefined. Examples of
 * this output include gfsh version --full and the log file preamble.
 */
public interface ComponentVersion {
  /**
   * Provides a string representation of the distribution or component name. The format of this
   * string is undefined but should be short and meaningful when displayed alone and unadorned with
   * version information.
   *
   * @return The component name. Some examples would be "Apache Geode" or "The Best IMDG".
   */
  @NotNull
  String getName();

  /**
   * Provides a string representation of the version. The format of this version is undefined but
   * should not adorned with names or prefixes like "Geode" or "v". Some examples:
   *
   * <pre>
   * "1.15.0-build.123", "1.15.1", or "1.2-rc1".
   * </pre>
   *
   * @return The version of this component.
   */
  @NotNull
  String getVersion();

  /**
   * Provides a string representation of all the version like information you might want to display
   * in the full version or log preamble. The format of this string should be key value pairs
   * separated by colons each on their own line. The basic version should be included in this output
   * but the name should not.
   * For example:
   *
   * <pre>
   *   Version: 1.15.0
   *   Repository: github.com/apache/geode
   *   Revision: f5584205b0ee93904a5f2a9921459f99a1caa515
   * </pre>
   *
   * @return Detailed component version information.
   */
  @NotNull
  Map<@NotNull String, @NotNull String> getDetails();
}
