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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.internal.DUnitSanctionedSerializablesService;
import org.apache.geode.test.dunit.internal.Master;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class AnalyzeDUnitSerializablesIntegrationTest extends AnalyzeSerializablesJUnitTestBase {
  private static final Logger logger = LogService.getLogger();

  private static final Set<Class<?>> IGNORE_CLASSES = new HashSet<>();
  static {
    IGNORE_CLASSES.add(Master.class);
    classForName("org.apache.geode.test.dunit.internal.RemoteDUnitVM")
        .ifPresent(IGNORE_CLASSES::add);
  }

  @Override
  protected String getModuleName() {
    return "geode-dunit";
  }

  @Override
  protected Class<?> getModuleClass() {
    return DUnitSanctionedSerializablesService.class;
  }

  @Override
  protected boolean ignoreClass(Class<?> theClass) {
    return IGNORE_CLASSES.contains(theClass);
  }

  private static Optional<Class<?>> classForName(String className) {
    try {
      return Optional.of(Class.forName(className));
    } catch (ClassNotFoundException e) {
      logger.error("Unable to add class {} to IGNORE_CLASSES", className, e);
    }
    return Optional.empty();
  }
}
