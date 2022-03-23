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
package org.apache.geode.codeAnalysis;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.PortfolioNoDS;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.apache.geode.test.junit.internal.JUnitSanctionedSerializablesService;

@Category(SerializationTest.class)
public class AnalyzeJUnitSerializablesIntegrationTest
    extends AnalyzeSerializablesWithClassAnalysisRuleTestBase {

  private static final Set<Class<?>> IGNORE_CLASSES = new HashSet<>(asList(
      PortfolioNoDS.class, PortfolioPdx.class));

  @Override
  protected String getModuleName() {
    return "geode-junit";
  }

  @Override
  protected Optional<Class<?>> getModuleClass() {
    return Optional.of(JUnitSanctionedSerializablesService.class);
  }

  @Override
  protected boolean ignoreClass(Class<?> theClass) {
    return IGNORE_CLASSES.contains(theClass);
  }
}
