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
package org.apache.geode.logging.internal.executors;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

// @RunWith(ArchUnitRunner.class)
// @AnalyzeClasses(packages = {"org.apache.geode.."})
public class LoggingDependenciesTest {
  @ArchTest
  public static final ArchRule noTransitiveDependenciesOnCore = classes()
      .that()
      .resideInAnyPackage("org.apache.geode.internal.logging..")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.internal.logging..")
              .or(not(resideInAPackage("org.apache.geode..")))

              // TODO: the following exceptions should be eliminated

              // e.g. this package depends on packages in geode-core which is bad
              .or(resideInAPackage("org.apache.geode.test..")));
}
