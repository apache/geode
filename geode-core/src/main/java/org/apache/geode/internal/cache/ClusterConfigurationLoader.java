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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.UnmodifiableException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.functions.GetClusterConfigurationFunction;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;

public class ClusterConfigurationLoader {

  private static final Logger logger = LogService.getLogger();

  /**
   * Deploys the jars received from shared configuration, it undeploys any other jars that were not
   * part of shared configuration
   *
   * @param cache Cache of this member
   * @param response {@link ConfigurationResponse} received from the locators
   */
  public void deployJarsReceivedFromClusterConfiguration(Cache cache,
      ConfigurationResponse response) throws IOException, ClassNotFoundException {
    logger.info("Requesting cluster configuration");
    if (response == null) {
      return;
    }

    String[] jarFileNames = response.getJarNames();
    byte[][] jarBytes = response.getJars();

    if (jarFileNames != null && jarBytes != null) {
      logger.info("Got response with jars: {}", Stream.of(jarFileNames).collect(joining(",")));
      JarDeployer jarDeployer = ClassPathLoader.getLatest().getJarDeployer();
      jarDeployer.suspendAll();
      try {
        List<String> extraJarsOnServer =
            jarDeployer.findDeployedJars().stream().map(DeployedJar::getJarName)
                .filter(jarName -> !ArrayUtils.contains(jarFileNames, jarName)).collect(toList());

        for (String extraJar : extraJarsOnServer) {
          logger.info("Removing jar not present in cluster configuration: {}", extraJar);
          jarDeployer.deleteAllVersionsOfJar(extraJar);
        }

        List<DeployedJar> deployedJars = jarDeployer.deploy(jarFileNames, jarBytes);

        deployedJars.stream().filter(Objects::nonNull)
            .forEach((jar) -> logger.info("Deployed: {}", jar.getFile().getAbsolutePath()));
      } finally {
        jarDeployer.resumeAll();
      }
    }
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
        requestedConfiguration.get(ClusterConfigurationService.CLUSTER_CONFIG);
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
        requestedConfiguration.get(ClusterConfigurationService.CLUSTER_CONFIG);
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
    GetClusterConfigurationFunction function = new GetClusterConfigurationFunction();

    ConfigurationResponse response = null;
    for (InternalDistributedMember locator : locatorList) {
      ResultCollector resultCollector =
          FunctionService.onMember(locator).setArguments(groups).execute(function);
      Object result = ((ArrayList) resultCollector.getResult()).get(0);
      if (result instanceof ConfigurationResponse) {
        response = (ConfigurationResponse) result;
        break;
      } else {
        logger.error("Received invalid result from {}: {}", locator.toString(), result);
        if (result instanceof Throwable) {
          // log the stack trace.
          logger.error(result.toString(), result);
        }
      }
    }

    // if the response is null
    if (response == null) {
      throw new ClusterConfigurationNotAvailableException(
          "Unable to retrieve cluster configuration from the locator.");
    }

    return response;
  }

  Set<String> getGroups(String groupString) {
    if (StringUtils.isBlank(groupString)) {
      return new HashSet<>();
    }

    return (Arrays.stream(groupString.split(",")).collect(Collectors.toSet()));
  }

}
