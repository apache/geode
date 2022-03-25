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
package org.apache.geode.distributed.internal.membership;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.junit.CacheMode;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePortHelper;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.membership.gms..",
    cacheMode = CacheMode.PER_CLASS,
    importOptions = ImportOption.DoNotIncludeArchives.class)
public class MembershipDependenciesJUnitTest {

  /*
   * This test verifies that packages defined in the geode-membership module depend only on
   * packages defined within that module or on packages defined outside the geode-core module
   * (org.apache.geode packages) or on packages defined in a handful of small "leaf" modules.
   *
   * The most important thing is to prevent geode-membership from depending on geode-core.
   */
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")
              .or(resideInAPackage("org.apache.geode.distributed.internal.membership.api.."))

              // OK to depend on these "leaf" dependencies
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.."))
              .or(resideInAPackage("org.apache.geode.distributed.internal.tcpserver.."))
              .or(resideInAPackage("org.apache.geode.internal.inet.."))
              .or(resideInAPackage("org.apache.geode.internal.lang.."))
              .or(resideInAPackage("org.apache.geode.annotations.."))
              .or(resideInAPackage("org.apache.geode.codeAnalysis.."))

              .or(not(resideInAPackage("org.apache.geode..")))
              .or(type(AvailablePortHelper.class))

              // TODO: we dursn't depend on the test package cause it depends on pkgs in geode-core
              .or(resideInAPackage("org.apache.geode.test..")));
}
