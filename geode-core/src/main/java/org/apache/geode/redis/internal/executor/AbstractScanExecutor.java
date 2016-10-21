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
package org.apache.geode.redis.internal.executor;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;


public abstract class AbstractScanExecutor extends AbstractExecutor {

  protected final String ERROR_CURSOR = "Invalid cursor";

  protected final String ERROR_COUNT = "Count must be numeric and positive";

  protected final String ERROR_INVALID_CURSOR =
      "Cursor is invalid, dataset may have been altered if this is cursor from a previous scan";

  protected final int DEFUALT_COUNT = 10;

  protected abstract List<?> getIteration(Collection<?> list, Pattern matchPatter, int count,
      int cursor);

  /**
   * @param pattern A glob pattern.
   * @return A regex pattern to recognize the given glob pattern.
   */
  protected final Pattern convertGlobToRegex(String pattern) {
    if (pattern == null)
      return null;
    return GlobPattern.compile(pattern);
  }
}
