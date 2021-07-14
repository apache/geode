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
package org.apache.geode.internal.cache;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.UnmodifiableException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.functions.DownloadJarFunction;
import org.apache.geode.management.internal.configuration.functions.GetClusterConfigurationFunction;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.management.internal.util.ManagementUtils;

public class ClusterConfigurationLoader {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final Function GET_CLUSTER_CONFIG_FUNCTION = new GetClusterConfigurationFunction();

  /**
   * Deploys the jars received from shared configuration, it undeploys any other jars that were not
   * part of shared configuration
   *
   * @param response {@link ConfigurationResponse} received from the locators
   */
  public void deployJarsReceivedFromClusterConfiguration(ConfigurationResponse response)
      throws IOException, ClassNotFoundException {
    if (response == null) {
      return;
    }

    logger.info("deploying jars received from cluster configuration");
    List<String> jarFileNames =
        response.getJarNames().values().stream()
            .filter(Objects::nonNull)
            .flatMap(Set::stream)
            .collect(Collectors.toList());

    if (!jarFileNames.isEmpty()) {
      logger.info("Got response with jars: {}", String.join(",", jarFileNames));
      JarDeploymentService jarDeploymentService =
          ClassPathLoader.getLatest().getJarDeploymentService();

      Set<File> stagedJarFiles =
          getJarsFromLocator(response.getMember(), response.getJarNames());
      for (File file : stagedJarFiles) {
        logger.info("Removing old versions of {} in cluster configuration.", file.getName());
        jarDeploymentService.undeploy(file.getName());
        jarDeploymentService.deploy(file);
        logger.info("Deployed: {}", file.getAbsolutePath());
      }
    }
  }

  // download the jars from the locator for the specific groups this server is on (the server
  // might be on multiple groups.
  private Set<File> getJarsFromLocator(DistributedMember locator,
      Map<String, Set<String>> jarNames) throws IOException {
    Map<String, File> results = new HashMap<>();

    for (String group : jarNames.keySet()) {
      for (String jar : jarNames.get(group)) {
        results.put(jar, downloadJar(locator, group, jar));
      }
    }

    return new HashSet<>(results.values());
  }

  // the returned File will use use jarName as the fileName
  public File downloadJar(DistributedMember locator, String groupName, String jarName)
      throws IOException {
    Path tempDir = FileUploader.createSecuredTempDirectory("deploy-");
    Path tempJar = Paths.get(tempDir.toString(), jarName);

    downloadTo(locator, groupName, jarName, tempJar);

    return tempJar.toFile();
  }

  void downloadTo(DistributedMember locator, String groupName, String jarName, Path jarPath)
      throws IOException {
    ResultCollector<RemoteInputStream, List<RemoteInputStream>> rc =
        (ResultCollector<RemoteInputStream, List<RemoteInputStream>>) ManagementUtils
            .executeFunction(
                new DownloadJarFunction(), new Object[] {groupName, jarName},
                Collections.singleton(locator));

    List<RemoteInputStream> result = rc.getResult();
    if (result.get(0) instanceof Throwable) {
      throw new IllegalStateException(((Throwable) result.get(0)).getMessage());
    }

    FileOutputStream fos = new FileOutputStream(jarPath.toString());

    InputStream jarStream = RemoteInputStreamClient.wrap(result.get(0));
    IOUtils.copyLarge(jarStream, fos);

    fos.close();
    jarStream.close();
  }

  /***
   * Apply the cache-xml cluster configuration on this member
   */
  public void applyClusterXmlConfiguration(Cache cache, ConfigurationResponse response,
      String groupList) {
    if (response == null || response.getRequestedConfiguration().isEmpty()) {
      return;
    }

    Set<String> groups = getGroups(groupList);
    Map<String, Configuration> requestedConfiguration = response.getRequestedConfiguration();

    List<String> cacheXmlContentList = new LinkedList<String>();

    // apply the cluster config first
    Configuration clusterConfiguration =
        requestedConfiguration.get(ConfigurationPersistenceService.CLUSTER_CONFIG);
    if (clusterConfiguration != null) {
      String cacheXmlContent = clusterConfiguration.getCacheXmlContent();
      if (StringUtils.isNotBlank(cacheXmlContent)) {
        cacheXmlContentList.add(cacheXmlContent);
      }
    }

    // then apply the groups config
    for (String group : groups) {
      Configuration groupConfiguration = requestedConfiguration.get(group);
      if (groupConfiguration != null) {
        String cacheXmlContent = groupConfiguration.getCacheXmlContent();
        if (StringUtils.isNotBlank(cacheXmlContent)) {
          cacheXmlContentList.add(cacheXmlContent);
        }
      }
    }

    // apply the requested cache xml
    for (String cacheXmlContent : cacheXmlContentList) {
      InputStream is = new ByteArrayInputStream(cacheXmlContent.getBytes());
      try {
        cache.loadCacheXml(is);
      } finally {
        try {
          is.close();
        } catch (IOException e) {
        }
      }
    }
  }

  /***
   * Apply the gemfire properties cluster configuration on this member
   *
   * @param response {@link ConfigurationResponse} containing the requested {@link Configuration}
   * @param config this member's config
   */
  public void applyClusterPropertiesConfiguration(ConfigurationResponse response,
      DistributionConfig config) {
    if (response == null || response.getRequestedConfiguration().isEmpty()) {
      return;
    }

    Set<String> groups = getGroups(config.getGroups());
    Map<String, Configuration> requestedConfiguration = response.getRequestedConfiguration();

    final Properties runtimeProps = new Properties();

    // apply the cluster config first
    Configuration clusterConfiguration =
        requestedConfiguration.get(ConfigurationPersistenceService.CLUSTER_CONFIG);
    if (clusterConfiguration != null) {
      runtimeProps.putAll(clusterConfiguration.getGemfireProperties());
    }

    final Properties groupProps = new Properties();

    // then apply the group config
    for (String group : groups) {
      Configuration groupConfiguration = requestedConfiguration.get(group);
      if (groupConfiguration != null) {
        for (Map.Entry<Object, Object> e : groupConfiguration.getGemfireProperties().entrySet()) {
          if (groupProps.containsKey(e.getKey())) {
            logger.warn("Conflicting property {} from group {}", e.getKey(), group);
          } else {
            groupProps.put(e.getKey(), e.getValue());
          }
        }
      }
    }

    runtimeProps.putAll(groupProps);

    Set<Object> attNames = runtimeProps.keySet();
    for (Object attNameObj : attNames) {
      String attName = (String) attNameObj;
      String attValue = runtimeProps.getProperty(attName);
      try {
        config.setAttribute(attName, attValue, ConfigSource.runtime());
      } catch (IllegalArgumentException e) {
        logger.info(e.getMessage());
      } catch (UnmodifiableException e) {
        logger.info(e.getMessage());
      }
    }
  }

  /**
   * Request the shared configuration for group(s) from locator(s) this member is bootstrapped with.
   *
   * This will request the group config this server belongs plus the "cluster" config
   *
   * @return {@link ConfigurationResponse}
   */
  public ConfigurationResponse requestConfigurationFromLocators(String groupList,
      Set<InternalDistributedMember> locatorList)
      throws ClusterConfigurationNotAvailableException, UnknownHostException {

    Set<String> groups = getGroups(groupList);

    ConfigurationResponse response = null;

    int attempts = 6;
    OUTER: while (attempts > 0) {
      for (InternalDistributedMember locator : locatorList) {
        logger.info("Attempting to retrieve cluster configuration from {} - {} attempts remaining",
            locator.getName(), attempts);
        response = requestConfigurationFromOneLocator(locator, groups);
        if (response != null) {
          break OUTER;
        }
      }

      try {
        Thread.sleep(10000);
      } catch (InterruptedException ex) {
        break;
      }

      attempts--;
    }

    // if the response is null
    if (response == null) {
      throw new ClusterConfigurationNotAvailableException(
          "Unable to retrieve cluster configuration from the locator.");
    }

    return response;
  }

  protected ConfigurationResponse requestConfigurationFromOneLocator(
      InternalDistributedMember locator, Set<String> groups) {
    ConfigurationResponse configResponse = null;

    try {
      ResultCollector resultCollector = FunctionService.onMember(locator).setArguments(groups)
          .execute(GET_CLUSTER_CONFIG_FUNCTION);
      Object result = ((ArrayList) resultCollector.getResult()).get(0);
      if (result instanceof ConfigurationResponse) {
        configResponse = (ConfigurationResponse) result;
        configResponse.setMember(locator);
      } else {
        if (result != null) {
          logger.error("Received invalid result from {}: {}", locator.toString(), result);
        }
        if (result instanceof Throwable) {
          // log the stack trace.
          logger.error(result.toString(), result);
        }
      }
    } catch (FunctionException fex) {
      // Rethrow unless we're possibly reconnecting
      if (!(fex.getCause() instanceof LockServiceDestroyedException
          || fex.getCause() instanceof FunctionInvocationTargetException)) {
        throw fex;
      }
    }

    return configResponse;
  }

  Set<String> getGroups(String groupString) {
    if (StringUtils.isBlank(groupString)) {
      return new HashSet<>();
    }

    return (Arrays.stream(groupString.split(",")).collect(Collectors.toSet()));
  }

}
