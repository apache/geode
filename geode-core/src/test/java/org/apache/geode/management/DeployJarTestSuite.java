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
package org.apache.geode.management;

import org.apache.geode.internal.ClassPathLoaderIntegrationTest;
import org.apache.geode.internal.ClassPathLoaderTest;
import org.apache.geode.internal.DeployedJarJUnitTest;
import org.apache.geode.internal.JarDeployerIntegrationTest;
import org.apache.geode.management.internal.cli.commands.DeployCommandRedeployDUnitTest;
import org.apache.geode.management.internal.cli.commands.DeployCommandsDUnitTest;
import org.apache.geode.management.internal.configuration.ClusterConfigDeployJarDUnitTest;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@Ignore
@RunWith(Suite.class)
@Suite.SuiteClasses({DeployedJarJUnitTest.class, DeployCommandsDUnitTest.class,
    JarDeployerIntegrationTest.class, ClassPathLoaderIntegrationTest.class,
    ClassPathLoaderTest.class, DeployCommandRedeployDUnitTest.class,
    ClusterConfigDeployJarDUnitTest.class})
public class DeployJarTestSuite {
}
