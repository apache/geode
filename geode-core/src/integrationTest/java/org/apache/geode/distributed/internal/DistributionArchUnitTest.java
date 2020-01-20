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
package org.apache.geode.distributed.internal;

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

import org.apache.geode.distributed.internal.membership.MembershipJUnitTest;
import org.apache.geode.distributed.internal.membership.api.Membership;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode", cacheMode = CacheMode.PER_CLASS,
    importOptions = ImportOption.DoNotIncludeArchives.class)
public class DistributionArchUnitTest {

  @ArchTest
  public static final ArchRule membershipShouldOnlyBeAccessedThroughDistributionClass = classes()
      .that(type(Membership.class))
      .should()
      .onlyBeAccessed()
      .byClassesThat(type(Distribution.class)
          .or(type(MembershipJUnitTest.class)) // another integrationTest
          .or(type(DistributionImpl.MyDCReceiver.class))
          .or(resideInAPackage("org.apache.geode.distributed.internal.membership.api.."))
          .or(resideInAPackage("org.apache.geode.distributed.internal.membership.gms.."))
          .or(resideInAPackage("org.apache.geode.internal.tcp.."))
          .or(resideInAPackage("org.apache.geode.distributed.internal.direct.."))
          .or(type(DistributionImpl.class))

  );
}
