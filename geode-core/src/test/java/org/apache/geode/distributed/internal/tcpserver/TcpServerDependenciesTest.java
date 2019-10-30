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
package org.apache.geode.distributed.internal.tcpserver;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;


@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.tcpserver")
public class TcpServerDependenciesTest {

  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")

      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(type(LogService.class))
              .or(type(LoggingExecutors.class))
              .or(type(LoggingThread.class))

              .or(not(resideInAPackage("org.apache.geode..")))
              .or(resideInAPackage("org.apache.geode.test.."))


              // TODO - serialization related classes
              .or(type(DataSerializer.class))
              .or(type(DataSerializable.class))

              // TODO - TCP socket related classes
              .or(type(SocketCreator.class))
              .or(type(SSLConfigurationFactory.class))
              .or(type(SecurableCommunicationChannel.class))
              .or(type(SocketCreatorFactory.class))
              .or(type(SSLConfigurationFactory.class))

              // TODO - cancel excpetion
              .or(type(CancelException.class))

              // TODO - config
              .or(type(DistributionConfigImpl.class))
              .or(type(DistributionConfig.class))


              // TODO - god classes
              .or(type(SystemFailure.class))


  );


}
