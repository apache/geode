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
package org.apache.geode.distributed.internal.membership.gms.api;

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

import org.apache.geode.distributed.LocatorIntegrationTest;
import org.apache.geode.distributed.internal.membership.MembershipJUnitTest;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode..",
    cacheMode = CacheMode.PER_CLASS,
    importOptions = ImportOption.DoNotIncludeArchives.class)
public class CoreOnlyUsesMembershipAPIArchUnitTest {

  @ArchTest
  public static final ArchRule coreOnlyUsesMembershipAPI = classes()
      .that()
      .resideInAPackage("org.apache.geode..")
      .and(not(resideInAPackage("org.apache.geode.distributed.internal.membership.adapter")))
      .and(not(resideInAPackage("org.apache.geode.distributed.internal.membership.gms..")))

      // These non-integration tests also need to be addressed
      // .and(not(type(GMSMembershipViewJUnitTest.class)))
      // .and(not(type(AbstractGMSAuthenticatorTestCase.class)))
      // .and(not(type(DistributionTest.class)))

      .and(not(type(MembershipJUnitTest.class)))
      .and(not(type(LocatorIntegrationTest.class)))

      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership.gms.api")
              .or(type(MembershipManagerHelper.class))
              .or(not(resideInAPackage("org.apache.geode.distributed.internal.membership.gms.."))));
}
