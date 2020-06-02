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
package org.apache.geode.internal.cache.execute.util;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;

/**
 * Utilities for casting and working around raw types in the {@link FunctionService} API.
 */
@SuppressWarnings({"unchecked", "unused"})
public class TypedFunctionService {

  protected TypedFunctionService() {
    // do not instantiate
  }

  /**
   * Adds parameterized type support to {@link FunctionService#onRegion(Region)}.
   */
  public static <IN, OUT, AGG> Execution<IN, OUT, AGG> onRegion(Region<?, ?> region) {
    return FunctionService.onRegion(region);
  }
}
