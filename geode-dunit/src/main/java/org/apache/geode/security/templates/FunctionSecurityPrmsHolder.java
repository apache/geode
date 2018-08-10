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
package org.apache.geode.security.templates;

import java.util.Set;

/**
 * This is a sample class for objects which hold information of the authorized function names and
 * authorized value for the {@code optimizeForWrite}.
 *
 * @since GemFire 6.0
 */
public class FunctionSecurityPrmsHolder {

  private final Boolean optimizeForWrite;
  private final Set<String> functionIds;
  private final Set<String> keySet;

  public FunctionSecurityPrmsHolder(final Boolean optimizeForWrite, final Set<String> functionIds,
      final Set<String> keySet) {
    this.optimizeForWrite = optimizeForWrite;
    this.functionIds = functionIds;
    this.keySet = keySet;
  }

  public Boolean isOptimizeForWrite() {
    return this.optimizeForWrite;
  }

  public Set<String> getFunctionIds() {
    return this.functionIds;
  }

  public Set<String> getKeySet() {
    return this.keySet;
  }
}
