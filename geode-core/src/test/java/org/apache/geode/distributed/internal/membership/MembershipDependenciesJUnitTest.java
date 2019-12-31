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
import com.tngtech.archunit.junit.ArchIgnore;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.junit.CacheMode;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.util.JavaWorkarounds;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.membership.gms..",
    cacheMode = CacheMode.PER_CLASS,
    importOptions = ImportOption.DoNotIncludeArchives.class)
public class MembershipDependenciesJUnitTest {

  /*
   * This test verifies that the membership component (which is currently made up of classes
   * inside the geode-core module, but which may someday reside in a separate module)
   * depends only on packages within itself (within the membership component) or packages
   * outside apache.geode.
   *
   * For purposes of this test, classes in the membership...adapter package are not considered.
   * They will eventually become part of geode-core.
   *
   * While this rule is ignored, comment-out the ignore annotation to run it periodically to get
   * the current count of deviations.
   */
  // TODO: remove ignore once membershipDoesntDependOnCoreProvisional matches this rule exactly
  @ArchIgnore
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCore = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")
      // .and()
      // .resideOutsideOfPackage("org.apache.geode.distributed.internal.membership.adapter..")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")

              // OK to depend on these "leaf" dependencies
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.log4j.api.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.executors.."))
              .or(resideInAPackage("org.apache.geode.distributed.internal.tcpserver.."))
              .or(resideInAPackage("org.apache.geode.distributed.internal.tcpserver.."))

              .or(not(resideInAPackage("org.apache.geode.."))));

  /*
   * This test is a work-in-progress. It starts from the membershipDoesntDependOnCore rule
   * and adds deviations. Each deviation has a comment like TODO:...
   * Those deviations comprise a to do list for the membership team as it modularizes
   * the membership component--severing its dependency on the geode-core component.
   */
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")

      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")

              // OK to depend on these "leaf" dependencies
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.."))
              .or(resideInAPackage("org.apache.geode.logging.internal.executors.."))
              .or(resideInAPackage("org.apache.geode.distributed.internal.tcpserver.."))
              .or(type(LocalHostUtil.class))

              .or(not(resideInAPackage("org.apache.geode..")))

              // TODO: we dursn't depend on the test package cause it depends on pkgs in geode-core
              .or(resideInAPackage("org.apache.geode.test.."))

              // TODO: Create a new stats interface for membership
              .or(type(LocatorStats.class))

              // TODO: break dependencies on locator-related classes
              .or(type(Locator.class))

              // TODO:
              .or(type(JavaWorkarounds.class))

  );
}
