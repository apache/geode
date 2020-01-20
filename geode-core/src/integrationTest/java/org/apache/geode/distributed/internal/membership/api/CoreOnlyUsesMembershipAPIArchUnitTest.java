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

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

/**
 * This test ensures that various geode packages do not access membership internals.
 * It is broken into multiple tests in order to keep memory use low. Analyzing all
 * geode classes in a single test requires 1.5g of heap.<br>
 * This test class can be removed if and when we create an isolated Java module that does
 * not export internal membership classes.
 */
public class CoreOnlyUsesMembershipAPIArchUnitTest {

  @Test
  public void distributedAndInternalClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.distributed..",
        "org.apache.geode.internal..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void cacheClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode",
        "org.apache.geode.cache..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void managementClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.management..",
        "org.apache.geode.admin..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void securityClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.security..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void pdxClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.pdx..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void exampleClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.examples..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  @Test
  public void miscCoreClassesDoNotUseMembershipInternals() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages(
        "org.apache.geode.alerting..",
        "org.apache.geode.compression..",
        "org.apache.geode.datasource..",
        "org.apache.geode.i18n..",
        "org.apache.geode.lang..",
        "org.apache.geode.logging..",
        "org.apache.geode.metrics..",
        "org.apache.geode.ra..",
        "org.apache.geode.distributed.internal.membership..");

    checkMembershipAPIUse(importedClasses);
  }

  private void checkMembershipAPIUse(JavaClasses importedClasses) {
    ArchRule myRule = layeredArchitecture()
        .layer("internal")
        .definedBy(resideInAPackage("org.apache.geode.distributed.internal.membership.gms.."))
        .layer("api").definedBy("org.apache.geode.distributed.internal.membership.api")
        .whereLayer("internal").mayOnlyBeAccessedByLayers("api");

    myRule.check(importedClasses);
  }
}
