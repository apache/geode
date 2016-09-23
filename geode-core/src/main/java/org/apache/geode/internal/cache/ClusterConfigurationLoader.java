/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.UnmodifiableException;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.JarClassLoader;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.process.ClusterConfigurationNotAvailableException;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.messages.ConfigurationRequest;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;

public class ClusterConfigurationLoader {
  
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Deploys the jars received from shared configuration, it undeploys any other jars that were not part of shared configuration 
   * @param cache Cache of this member
   * @param response {@link ConfigurationResponse} received from the locators 
   * @throws IOException 
   * @throws ClassNotFoundException 
   */
  public static void deployJarsReceivedFromClusterConfiguration(Cache cache , ConfigurationResponse response) throws IOException, ClassNotFoundException {
    if(response == null)
      return;

    String []jarFileNames = response.getJarNames();
    byte [][]jarBytes = response.getJars();
    
    final JarDeployer jarDeployer = new JarDeployer(((GemFireCacheImpl) cache).getDistributedSystem().getConfig().getDeployWorkingDir());

    /******
     * Un-deploy the existing jars, deployed during cache creation, do not delete anything
     */

    if (jarFileNames != null && jarBytes != null) {
      JarClassLoader[] jarClassLoaders = jarDeployer.deploy(jarFileNames, jarBytes);
      for (int i = 0; i < jarFileNames.length; i++) {
        if (jarClassLoaders[i] != null) {
          logger.info("Deployed " + (jarClassLoaders[i].getFileCanonicalPath()));
        } 
      }
    }
  }

  /***
   * Apply the cache-xml cluster configuration on this member
   * @param cache Cache created for this member
   * @param response {@link ConfigurationResponse} containing the requested {@link Configuration}
   * @param config this member's config.
   */
  public static void applyClusterXmlConfiguration(Cache cache , ConfigurationResponse response, DistributionConfig config) {
    if(response == null || response.getRequestedConfiguration().isEmpty())
      return;

    List<String> groups = getGroups(config);
    Map<String, Configuration> requestedConfiguration = response.getRequestedConfiguration();

    List<String> cacheXmlContentList = new LinkedList<String>();

    // apply the cluster config first
    Configuration clusterConfiguration = requestedConfiguration.get(SharedConfiguration.CLUSTER_CONFIG);
    if (clusterConfiguration != null) {
      String cacheXmlContent = clusterConfiguration.getCacheXmlContent();
      if (!StringUtils.isBlank(cacheXmlContent)) {
        cacheXmlContentList.add(cacheXmlContent);
      }
    }

    // then apply the groups config
    for (String group : groups) {
      Configuration groupConfiguration = requestedConfiguration.get(group);
      if (groupConfiguration != null) {
        String cacheXmlContent = groupConfiguration.getCacheXmlContent();
        if (!StringUtils.isBlank(cacheXmlContent)) {
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
   * @param cache Cache created for this member
   * @param response {@link ConfigurationResponse} containing the requested {@link Configuration}
   * @param config this member's config
   */
  public static void applyClusterPropertiesConfiguration(Cache cache , ConfigurationResponse response, DistributionConfig config) {
    if(response == null || response.getRequestedConfiguration().isEmpty())
      return;

    List<String> groups = getGroups(config);
    Map<String, Configuration> requestedConfiguration = response.getRequestedConfiguration();

    final Properties runtimeProps = new Properties();

    // apply the cluster config first
    Configuration clusterConfiguration = requestedConfiguration.get(SharedConfiguration.CLUSTER_CONFIG);
    if (clusterConfiguration != null) {
      runtimeProps.putAll(clusterConfiguration.getGemfireProperties());
    }

    // then apply the group config
    for (String group : groups) {
      Configuration groupConfiguration = requestedConfiguration.get(group);
      if (groupConfiguration != null) {
        runtimeProps.putAll(groupConfiguration.getGemfireProperties());
      }
    }

    Set<Object> attNames = runtimeProps.keySet();
    for (Object attNameObj : attNames) {
      String attName = (String) attNameObj;
      String attValue = runtimeProps.getProperty(attName) ;
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
   * @param config this member's configuration.
   * @return {@link ConfigurationResponse}
   * @throws ClusterConfigurationNotAvailableException 
   * @throws UnknownHostException 
   */
  public static ConfigurationResponse requestConfigurationFromLocators(DistributionConfig config, List<String> locatorList) throws ClusterConfigurationNotAvailableException, UnknownHostException {
    List<String> groups = ClusterConfigurationLoader.getGroups(config);
    ConfigurationRequest request = new ConfigurationRequest();

    request.addGroups(SharedConfiguration.CLUSTER_CONFIG);
    for (String group : groups) {
      request.addGroups(group);
    }
    
    request.setNumAttempts(10);
    
    ConfigurationResponse response = null;
    //Try talking to all the locators in the list
    //to get the shared configuration.

    TcpClient client = new TcpClient();

    for (String locatorInfo : locatorList) {
      DistributionLocatorId dlId = new DistributionLocatorId(locatorInfo);
      String ipaddress = dlId.getBindAddress();
      InetAddress locatorInetAddress = null;
      
      if (!StringUtils.isBlank(ipaddress)) {
        locatorInetAddress = InetAddress.getByName(ipaddress);
      } else {
        locatorInetAddress = dlId.getHost();
      }
      
      int port = dlId.getPort();
        
      try {
          response = (ConfigurationResponse)client.requestToServer(locatorInetAddress, port, request, 10000);
        } catch (UnknownHostException e) {
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Log
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    // if the response is null , that means Shared Configuration service is not installed on the locator
    // and hence it returns null
    
    if (response == null || response.failedToGetSharedConfig()) {
      throw new ClusterConfigurationNotAvailableException(LocalizedStrings.Launcher_Command_FAILED_TO_GET_SHARED_CONFIGURATION.toLocalizedString());
    } 

    return response;
  }

  private static List<String> getGroups(DistributionConfig config) {
    String groupString = config.getGroups();
    List<String> groups = new ArrayList<String>();
    if (!StringUtils.isBlank(groupString)) {
      groups.addAll((Arrays.asList(groupString.split(","))));
    }
    return groups;
  }
 
 /***
  * Get the host and port information of the locators 
  * @return List made up of a String array containing host and port 
  */
 public static List<String[]> getLocatorsInfo(String locatorsString) {

   List<String[]> locatorList = new ArrayList<String[]>();

   if (!StringUtils.isBlank(locatorsString)) {
     String[] bootstrappedlocators = locatorsString.split(",");
     for (String bootstrappedlocator : bootstrappedlocators) {
       locatorList.add(bootstrappedlocator.split("\\[|]"));
     }
   }
   return locatorList;
 }
 
 public static List<String[]> getLocatorsInfo(List<String> locatorConnectionStrings) {
   List<String[]> locatorList = new ArrayList<String[]>();
   
   for (String locatorConnectionString : locatorConnectionStrings) {
     locatorList.add(locatorConnectionString.split("\\[|]"));
   }
   return locatorList;
 }
 
 
}
