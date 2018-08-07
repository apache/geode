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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;

import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public abstract class ClusterConfigTestBase {
  public String clusterConfigZipPath;

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
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  protected Properties locatorProps;
  protected Properties serverProps;

  @Before
  public void beforeClusterConfigTestBase() throws Exception {
    clusterConfigZipPath = buildClusterZipFile();
    locatorProps = new Properties();
    serverProps = new Properties();

    // the following are default values, we don't need to set them. We do it for clarity purpose
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
  }

  private String buildClusterZipFile() throws Exception {
    File clusterConfigDir = temporaryFolder.newFolder("cluster_config");

    File clusterDir = new File(clusterConfigDir, "cluster");
    String clusterXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
        + "<region name=\"regionForCluster\">\n"
        + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
        + "  </region>\n" + "</cache>\n";
    writeFile(clusterDir, "cluster.xml", clusterXml);
    writeFile(clusterDir, "cluster.properties", "log-file-size-limit=5000");
    createJarFileWithClass("Cluster", "cluster.jar", clusterDir);

    File group1Dir = new File(clusterConfigDir, "group1");
    String group1Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
        + "<region name=\"regionForGroup1\">\n"
        + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
        + "  </region>\n" + "</cache>\n";
    writeFile(group1Dir, "group1.xml", group1Xml);
    writeFile(group1Dir, "group1.properties", "log-file-size-limit=6000");
    createJarFileWithClass("Group1", "group1.jar", group1Dir);


    File group2Dir = new File(clusterConfigDir, "group2");
    String group2Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
        + "<region name=\"regionForGroup2\">\n"
        + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
        + "  </region>\n" + "</cache>\n";
    writeFile(group2Dir, "group2.xml", group2Xml);
    writeFile(group2Dir, "group2.properties", "log-file-size-limit=7000");
    createJarFileWithClass("Group2", "group2.jar", group2Dir);


    File clusterConfigZip = new File(temporaryFolder.newFolder(), "cluster_config.zip");
    ZipUtils.zipDirectory(clusterConfigDir.getCanonicalPath(), clusterConfigZip.getCanonicalPath());

    FileUtils.deleteDirectory(clusterConfigDir);
    return clusterConfigZip.getCanonicalPath();
  }

  private File writeFile(File dir, String fileName, String content) throws IOException {
    dir.mkdirs();
    File file = new File(dir, fileName);
    FileUtils.writeStringToFile(file, content);

    return file;
  }

  protected String createJarFileWithClass(String className, String jarName, File dir)
      throws IOException {
    File jarFile = new File(dir, jarName);
    new ClassBuilder().writeJarFromName(className, jarFile);
    return jarFile.getCanonicalPath();
  }
}
