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
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.junit.CacheMode;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.membership.gms.MemberDataBuilderImpl;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.membership.gms.MembershipBuilderImpl;
import org.apache.geode.distributed.internal.membership.gms.MembershipLocatorBuilderImpl;
import org.apache.geode.internal.inet.LocalHostUtil;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.membership.api",
    cacheMode = CacheMode.PER_CLASS,
    importOptions = ImportOption.DoNotIncludeArchives.class)
public class MembershipAPIArchUnitTest {

  @ArchTest
  public static final ArchRule membershipAPIDoesntDependOnMembershipORCore = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership.api")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership.api")
              .or(not(resideInAPackage("org.apache.geode..")))
              // Serialization is a dependency of membership
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(resideInAPackage("org.apache.geode.distributed.internal.tcpserver.."))
              // allowed implementation class accesses
              .or(type(MembershipBuilderImpl.class))
              .or(type(MembershipLocatorBuilderImpl.class))
              .or(type(MemberDataBuilderImpl.class))
              .or(type(MemberIdentifierImpl.class))

              .or(type(LocalHostUtil.class)));
}
