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

package org.apache.geode.management.internal.cli.commands;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.configuration.RegionConfig;

class IndexDefinition {
  @Immutable
  static final Set<RegionConfig.Index> indexDefinitions =
      Collections.synchronizedSet(new HashSet<>());

  @SuppressWarnings("deprecation")
  static void addIndex(String name, String expression, String fromClause,
      org.apache.geode.cache.query.IndexType type) {
    RegionConfig.Index index = new RegionConfig.Index();
    index.setName(name);
    index.setFromClause(fromClause);
    index.setType(type.getName());
    index.setExpression(expression);
    indexDefinitions.add(index);

  }
}
