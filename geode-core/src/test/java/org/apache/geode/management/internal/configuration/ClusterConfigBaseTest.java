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
 *
 */

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;

import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.junit.Before;
import org.junit.Rule;

import java.util.Properties;

public class ClusterConfigBaseTest extends JUnit4DistributedTestCase {
  public static final String EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME = "cluster_config.zip";
  public static final String EXPORTED_CLUSTER_CONFIG_PATH =
      ClusterConfigBaseTest.class.getResource(EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME).getPath();

  public static final ConfigGroup CLUSTER = new ConfigGroup("cluster").regions("regionForCluster")
      .jars("cluster.jar").maxLogFileSize("5000").configFiles("cluster.properties", "cluster.xml");
  public static final ConfigGroup GROUP1 = new ConfigGroup("group1").regions("regionForGroup1")
      .jars("group1.jar").maxLogFileSize("6000").configFiles("group1.properties", "group1.xml");
  public static final ConfigGroup GROUP2 = new ConfigGroup("group2").regions("regionForGroup2")
      .jars("group2.jar").maxLogFileSize("7000").configFiles("group2.properties", "group2.xml");

  public static final ClusterConfig CONFIG_FROM_ZIP = new ClusterConfig(CLUSTER, GROUP1, GROUP2);

  public static final ClusterConfig REPLICATED_CONFIG_FROM_ZIP = new ClusterConfig(
      new ConfigGroup("cluster").maxLogFileSize("5000").jars("cluster.jar")
          .regions("regionForCluster"),
      new ConfigGroup("group1").maxLogFileSize("6000").jars("group1.jar")
          .regions("regionForGroup1"),
      new ConfigGroup("group2").maxLogFileSize("7000").jars("group2.jar")
          .regions("regionForGroup2"));

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  protected Properties locatorProps;
  protected Properties serverProps;

  @Before
  public void before() throws Exception {
    locatorProps = new Properties();
    serverProps = new Properties();

    // the following are default values, we don't need to set them. We do it for clarity purpose
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
  }
}
