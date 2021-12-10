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
package org.apache.geode.internal.serialization;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeArchives;
import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.junit.CacheMode;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(
    packages = "org.apache.geode.internal.serialization..",
    cacheMode = CacheMode.PER_CLASS,
    importOptions = {DoNotIncludeArchives.class, DoNotIncludeTests.class})
public class SerializationDependenciesTest {

  @ArchTest
  public static final ArchRule serializationDoesNotDependOnCore = classes()
      .that()
      .resideInAPackage("org.apache.geode.internal.serialization..")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.internal.serialization..")
              .or(not(resideInAPackage("org.apache.geode..")))
              .or(resideInAPackage("org.apache.geode.annotations.."))
              .or(resideInAPackage("org.apache.geode.internal.lang.utils.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.log4j.api.."))
              .or(resideInAPackage("org.apache.geode.test..")));
}
