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
package org.apache.geode.redis.internal.executor.key;

import java.math.BigInteger;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.GlobPattern;

public abstract class AbstractScanExecutor extends AbstractExecutor {

  protected final BigInteger UNSIGNED_LONG_CAPACITY = new BigInteger("18446744073709551615");
  protected final int DEFAULT_COUNT = 10;

  /**
   * @param pattern A glob pattern.
   * @return A regex pattern to recognize the given glob pattern.
   */
  protected Pattern convertGlobToRegex(String pattern) {
    if (pattern == null) {
      return null;
    }
    return GlobPattern.compile(pattern);
  }
}
