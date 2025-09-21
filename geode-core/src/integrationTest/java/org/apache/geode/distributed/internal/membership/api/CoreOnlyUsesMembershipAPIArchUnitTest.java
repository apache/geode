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
package org.apache.geode.distributed.internal.membership.api;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import java.util.regex.Pattern;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

/**
 * This test ensures that various geode packages do not access membership internals.
 * It is broken into multiple tests in order to keep memory use low. Analyzing all
 * geode classes in a single test requires 1.5g of heap.<br>
 * This test class can be removed if and when we create an isolated Java module that does
 * not export internal membership classes.
 * 
 * ARCHITECTURAL CHANGE NOTE: This test was updated to fix the "Layer 'api' is empty, Layer 'internal' is empty"
 * error. The original layered architecture approach failed because membership classes were moved from geode-core
 * to geode-membership module, leaving empty layers. The solution uses direct dependency rules instead of layered
 * architecture to enforce the same constraint: geode-core classes should not directly access GMS internals.
 */
public class CoreOnlyUsesMembershipAPIArchUnitTest {

  @Test
  public void distributedAndInternalClassesDoNotUseMembershipInternals() {
    // CHANGE: Removed membership package import - these classes are now in geode-membership module
    // REASON: Importing "org.apache.geode.distributed.internal.membership.." from geode-core finds no classes
    // since membership was extracted to separate module, causing empty layers in layered architecture rule
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.distributed..",
        "org.apache.geode.internal..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void geodeClassesDoNotUseMembershipInternals() {
    ClassFileImporter classFileImporter = getClassFileImporter();
    // create an ImportOption that only allows org.apache.geode classes and
    // membership classes. Prepackaged ImportOptions always cause a package's
    // subpackages to be walked with walkFileTree so you can't examine a single
    // high-level pachage like org.apache.geode without examining its subpackages.
    classFileImporter = classFileImporter.withImportOption(new ImportOption() {
      final Pattern matcher = Pattern.compile(".*/org/apache/geode/[a-zA-z0-9]+/.*");

      @Override
      public boolean includes(Location location) {
        // CHANGE: Removed membership package inclusion check
        // REASON: "org/apache/geode/distributed/internal/membership" no longer exists in geode-core,
        // so checking for it would always return false and serve no purpose
        return !location.matches(matcher);
      }
    });
    // CHANGE: Removed membership package from import list 
    // REASON: Same as above - membership packages moved to geode-membership module
    JavaClasses importedClasses = classFileImporter.importPackages("org.apache.geode");
    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void cacheClassesDoNotUseMembershipInternals() {
    // CHANGE: Removed membership package import, only analyze cache classes
    // REASON: Cache classes are the ones we want to test for architectural violations.
    // Membership packages don't exist in geode-core anymore, so importing them creates empty layers
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.cache..");

    // Check that cache classes do not directly depend on GMS internal classes
    ArchRule rule = classes()
        .that().resideInAPackage("org.apache.geode.cache..")
        .should().onlyDependOnClassesThat(
            not(resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")));

    rule.check(importedClasses);
  }

  @Test
  public void managementClassesDoNotUseMembershipInternals() {
    // CHANGE: Removed membership package imports from all remaining test methods
    // REASON: Consistent with other methods - membership packages moved to geode-membership module
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.management..",
        "org.apache.geode.admin..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void securityClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.security..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void pdxClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.pdx..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void exampleClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.examples..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void miscCoreClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = getClassFileImporter().importPackages(
        "org.apache.geode.alerting..",
        "org.apache.geode.compression..",
        "org.apache.geode.datasource..",
        "org.apache.geode.i18n..",
        "org.apache.geode.lang..",
        "org.apache.geode.logging..",
        "org.apache.geode.metrics..",
        "org.apache.geode.ra..");

    checkMembershipAPIUse(importedClasses);
  }

  private void checkMembershipAPIUse(JavaClasses importedClasses) {
    // CHANGE: Replaced layered architecture rule with direct dependency rule
    // REASON: Original layered architecture approach failed because:
    // 1. Layer 'internal' (membership.gms..) was empty - classes moved to geode-membership module
    // 2. Layer 'api' (membership.api) was empty - classes moved to geode-membership module
    // 3. Empty layers cause ArchUnit to throw "Architecture Violation" errors
    // 
    // NEW APPROACH: Direct dependency rule achieves same goal - ensures geode-core classes
    // cannot directly depend on GMS internal classes, while working with current module structure
    ArchRule myRule = classes()
        .that().resideInAPackage("org.apache.geode..")
        .should().onlyDependOnClassesThat(
            not(resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")));

    myRule.check(importedClasses);
  }

  private ClassFileImporter getClassFileImporter() {
    // CHANGE: Simplified to use default ClassFileImporter without custom ImportOptions
    // REASON: Previous implementation tried to exclude test classes but we need to ensure
    // JAR files (containing geode-membership classes) can be scanned for dependency analysis.
    // Default importer includes JARs on classpath, allowing ArchUnit to detect violations
    // when geode-core classes inappropriately depend on GMS internal classes from geode-membership module.
    return new ClassFileImporter();
  }


}
