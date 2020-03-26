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

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import joptsimple.internal.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.ClusterConfigurationLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.callbacks.ConfigurationChangeListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;

public class InternalConfigurationPersistenceService implements ConfigurationPersistenceService {
  private static final Logger logger = LogService.getLogger();

  /**
   * Name of the directory where the shared configuration artifacts are stored
   */
  public static final String CLUSTER_CONFIG_ARTIFACTS_DIR_NAME = "cluster_config";

  private static final String CLUSTER_CONFIG_DISK_STORE_NAME = "cluster_config";

  public static final String CLUSTER_CONFIG_DISK_DIR_PREFIX = "ConfigDiskDir_";

  /**
   * Name of the lock service used for shared configuration
   */
  private static final String SHARED_CONFIG_LOCK_SERVICE_NAME = "__CLUSTER_CONFIG_LS";

  /**
   * Name of the lock for locking the shared configuration
   */
  private static final String SHARED_CONFIG_LOCK_NAME = "__CLUSTER_CONFIG_LOCK";

  /**
   * Name of the region which is used to store the configuration information
   */
  public static final String CONFIG_REGION_NAME = "_ConfigurationRegion";
  private static final String CACHE_CONFIG_VERSION = "1.0";

  private final Path configDirPath;
  private final Path configDiskDirPath;

  private final Set<PersistentMemberPattern> newerSharedConfigurationLocatorInfo = new HashSet<>();
  private final AtomicReference<SharedConfigurationStatus> status = new AtomicReference<>();

  private final InternalCache cache;
  private final DistributedLockService sharedConfigLockingService;
  private final JAXBService jaxbService;

  public InternalConfigurationPersistenceService(InternalCache cache, Path workingDirectory,
      JAXBService jaxbService) {
    this(cache,
        sharedConfigLockService(cache.getDistributedSystem()),
        jaxbService,
        workingDirectory.resolve(CLUSTER_CONFIG_ARTIFACTS_DIR_NAME),
        workingDirectory
            .resolve(CLUSTER_CONFIG_DISK_DIR_PREFIX + cache.getDistributedSystem().getName()));
  }

  @VisibleForTesting
  public InternalConfigurationPersistenceService(JAXBService jaxbService) {
    this(null, null, jaxbService, null, null);
  }

  @VisibleForTesting
  InternalConfigurationPersistenceService(InternalCache cache,
      DistributedLockService sharedConfigLockingService, JAXBService jaxbService,
      Path configDirPath, Path configDiskDirPath) {
    this.cache = cache;
    this.configDirPath = configDirPath;
    this.configDiskDirPath = configDiskDirPath;
    this.sharedConfigLockingService = sharedConfigLockingService;
    status.set(SharedConfigurationStatus.NOT_STARTED);
    this.jaxbService = jaxbService;
  }


  /**
   * Gets or creates (if not created) shared configuration lock service
   */
  private static DistributedLockService sharedConfigLockService(DistributedSystem ds) {
    DistributedLockService sharedConfigDls =
        DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    try {
      if (sharedConfigDls == null) {
        sharedConfigDls = DLockService.create(SHARED_CONFIG_LOCK_SERVICE_NAME,
            (InternalDistributedSystem) ds, true, true);
      }
    } catch (IllegalArgumentException ignore) {
      return DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    }
    return sharedConfigDls;
  }

  public JAXBService getJaxbService() {
    return jaxbService;
  }

  /**
   * Adds/replaces the xml entity in the shared configuration we don't need to trigger the change
   * listener for this modification, so it's ok to operate on the original configuration object
   */
  public void addXmlEntity(XmlEntity xmlEntity, String[] groups) {
    lockSharedConfiguration();
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : listOf(groups)) {
        Configuration configuration = configRegion.get(group);
        if (configuration == null) {
          configuration = new Configuration(group);
        }
        String xmlContent = configuration.getCacheXmlContent();
        if (xmlContent == null || xmlContent.isEmpty()) {
          xmlContent = generateInitialXmlContent();
        }
        try {
          final Document doc = XmlUtils.createAndUpgradeDocumentFromXml(xmlContent);
          XmlUtils.addNewNode(doc, xmlEntity);
          configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
          configRegion.put(group, configuration);
        } catch (Exception e) {
          logger.error("error updating cluster configuration for group {}", group, e);
        }
      }
    } finally {
      unlockSharedConfiguration();
    }
  }

  /**
   * Deletes the xml entity from the shared configuration.
   */
  public void deleteXmlEntity(final XmlEntity xmlEntity, String[] groups) {
    lockSharedConfiguration();
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      // No group is specified, so delete in every single group if it exists.
      if (groups == null) {
        Set<String> groupSet = configRegion.keySet();
        groups = groupSet.toArray(new String[0]);
      }
      for (String group : groups) {
        Configuration configuration = configRegion.get(group);
        if (configuration != null) {
          String xmlContent = configuration.getCacheXmlContent();
          try {
            if (xmlContent != null && !xmlContent.isEmpty()) {
              Document doc = XmlUtils.createAndUpgradeDocumentFromXml(xmlContent);
              XmlUtils.deleteNode(doc, xmlEntity);
              configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
              configRegion.put(group, configuration);
            }
          } catch (Exception e) {
            logger.error("error updating cluster configuration for group {}", group, e);
          }
        }
      }
    } finally {
      unlockSharedConfiguration();
    }
  }

  /**
   * we don't need to trigger the change listener for this modification, so it's ok to operate on
   * the original configuration object
   */
  public void modifyXmlAndProperties(Properties properties, XmlEntity xmlEntity, String[] groups) {
    lockSharedConfiguration();
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : listOf(groups)) {
        Configuration configuration = configRegion.get(group);
        if (configuration == null) {
          configuration = new Configuration(group);
        }

        if (xmlEntity != null) {
          String xmlContent = configuration.getCacheXmlContent();
          if (xmlContent == null || xmlContent.isEmpty()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            CacheXmlGenerator.generateDefault(pw);
            xmlContent = sw.toString();
          }
          try {
            Document doc = XmlUtils.createAndUpgradeDocumentFromXml(xmlContent);
            // Modify the cache attributes
            XmlUtils.modifyRootAttributes(doc, xmlEntity);
            // Change the xml content of the configuration and put it the config region
            configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
          } catch (Exception e) {
            logger.error("error updating cluster configuration for group {}", group, e);
          }
        }

        if (properties != null) {
          configuration.getGemfireProperties().putAll(properties);
        }
        configRegion.put(group, configuration);
      }
    } finally {
      unlockSharedConfiguration();
    }
  }

  /**
   * Add jar information into the shared configuration and save the jars in the file system used
   * when deploying jars
   */
  public void addJarsToThisLocator(List<String> jarFullPaths, String[] groups) throws IOException {
    addJarsToThisLocator(getDeployedBy(), Instant.now().toString(), jarFullPaths, groups);
  }

  @VisibleForTesting
  void addJarsToThisLocator(String deployedBy, String deployedTime,
      List<String> jarFullPaths, String[] groups) throws IOException {
    lockSharedConfiguration();
    try {
      addJarsToGroups(listOf(groups), jarFullPaths, deployedBy, deployedTime);
    } finally {
      unlockSharedConfiguration();
    }
  }

  private void addJarsToGroups(List<String> groups, List<String> jarFullPaths, String deployedBy,
      String deployedTime) throws IOException {
    for (String group : groups) {
      copyJarsToGroupDir(group, jarFullPaths);
      addJarsToGroupConfig(group, jarFullPaths, deployedBy, deployedTime);
    }
  }

  private void addJarsToGroupConfig(String group, List<String> jarFullPaths, String deployedBy,
      String deployedTime) throws IOException {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    Configuration configuration = getConfigurationCopy(configRegion, group);

    jarFullPaths.stream()
        .map(toFileName())
        .map(jarFileName -> new Deployment(jarFileName, deployedBy, deployedTime))
        .forEach(configuration::putDeployment);

    String memberId = cache.getMyId().getId();
    configRegion.put(group, configuration, memberId);
  }

  private static List<String> listOf(String[] groups) {
    if (groups == null || groups.length == 0) {
      return Collections.singletonList(ConfigurationPersistenceService.CLUSTER_CONFIG);
    }
    return asList(groups);
  }

  private static Function<String, String> toFileName() {
    return fullPath -> Paths.get(fullPath).getFileName().toString();
  }

  private void copyJarsToGroupDir(String group, List<String> jarFullPaths) throws IOException {
    Path groupDir = configDirPath.resolve(group);
    for (String jarFullPath : jarFullPaths) {
      File stagedJarFile = new File(jarFullPath);
      String jarFileName = stagedJarFile.getName();
      Path destinationJarPath = groupDir.resolve(jarFileName);
      FileUtils.copyFile(stagedJarFile, destinationJarPath.toFile());
      removeOtherVersionsOf(groupDir, jarFileName);
    }
  }

  private static void removeOtherVersionsOf(Path groupDir, String jarFileName) throws IOException {
    String artifactId = JarDeployer.getArtifactId(jarFileName);
    for (File file : groupDir.toFile().listFiles()) {
      if (file.getName().equals(jarFileName)) {
        continue;
      }
      if (JarDeployer.getArtifactId(file.getName()).equals(artifactId)) {
        FileUtils.deleteQuietly(file);
      }
    }
  }

  private Configuration getConfigurationCopy(Region<String, Configuration> configRegion,
      String group) throws IOException {
    Configuration configuration = configRegion.get(group);

    if (configuration == null) {
      configuration = new Configuration(group);
      createConfigDirIfNecessary(group);
    } else {
      configuration = new Configuration(configuration);
    }
    return configuration;
  }

  /**
   * Removes the jar files from the shared configuration. used when undeploy jars
   *
   * @param jarNames Names of the jar files.
   * @param groups Names of the groups which had the jar file deployed.
   * @return true on success.
   */
  public boolean removeJars(final String[] jarNames, String[] groups) {
    lockSharedConfiguration();
    boolean success = true;
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      if (groups == null) {
        groups = configRegion.keySet().stream().toArray(String[]::new);
      }
      for (String group : groups) {
        Configuration configuration = configRegion.get(group);
        if (configuration == null) {
          break;
        }

        for (String jarRemoved : jarNames) {
          File jar = getPathToJarOnThisLocator(group, jarRemoved).toFile();
          if (jar.exists()) {
            try {
              FileUtils.forceDelete(jar);
            } catch (IOException e) {
              logger.error(
                  "Exception occurred while attempting to delete a jar from the filesystem: {}",
                  jarRemoved, e);
            }
          }
        }

        Configuration configurationCopy = new Configuration(configuration);
        configurationCopy.removeJarNames(jarNames);
        configRegion.put(group, configurationCopy);
      }
    } catch (Exception e) {
      logger.info("Exception occurred while deleting the jar files", e);
      success = false;
    } finally {
      unlockSharedConfiguration();
    }
    return success;
  }

  // Only used when a locator is initially starting up
  public void downloadJarFromOtherLocators(String groupName, String jarName)
      throws IllegalStateException, IOException {
    logger.info("Getting Jar files from other locators");
    DistributionManager dm = cache.getDistributionManager();
    DistributedMember me = cache.getMyId();
    List<DistributedMember> locators =
        new ArrayList<>(dm.getAllHostedLocatorsWithSharedConfiguration().keySet());
    locators.remove(me);

    createConfigDirIfNecessary(groupName);

    if (locators.isEmpty()) {
      throw new IllegalStateException(
          "Request to download jar " + jarName + " but no other locators are present");
    }

    downloadJarFromLocator(groupName, jarName, locators.get(0));
  }

  // used in the cluster config change listener when jarnames are changed in the internal region
  public void downloadJarFromLocator(String groupName, String jarName,
      DistributedMember sourceLocator) throws IllegalStateException, IOException {
    logger.info("Downloading jar {} from locator {}", jarName, sourceLocator.getName());

    createConfigDirIfNecessary(groupName);

    File jarFile = downloadJar(sourceLocator, groupName, jarName);

    File jarToWrite = getPathToJarOnThisLocator(groupName, jarName).toFile();
    FileUtils.copyFile(jarFile, jarToWrite);
  }

  /**
   * Retrieve a deployed jar from a locator. The retrieved file is staged in a temporary location.
   *
   * @param locator the DistributedMember
   * @param groupName the group to use when retrieving the jar
   * @param jarName the name of the deployed jar
   * @return a File referencing the downloaded jar. The File is downloaded to a temporary location.
   */
  public File downloadJar(DistributedMember locator, String groupName, String jarName)
      throws IOException {
    ClusterConfigurationLoader loader = new ClusterConfigurationLoader();
    return loader.downloadJar(locator, groupName, jarName);
  }

  /**
   * Creates the shared configuration service
   *
   * @param loadSharedConfigFromDir when set to true, loads the configuration from the share_config
   *        directory
   */
  void initSharedConfiguration(boolean loadSharedConfigFromDir) throws IOException {
    status.set(SharedConfigurationStatus.STARTED);
    Region<String, Configuration> configRegion = getConfigurationRegion();
    lockSharedConfiguration();
    try {
      removeInvalidXmlConfigurations(configRegion);
      if (loadSharedConfigFromDir) {
        logger.info("Reading cluster configuration from '{}' directory",
            InternalConfigurationPersistenceService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
        loadSharedConfigurationFromDir(configDirPath.toFile());
      } else {
        persistSecuritySettings(configRegion);
        // for those groups that have jar files, need to download the jars from other locators
        // if it doesn't exist yet
        for (Entry<String, Configuration> stringConfigurationEntry : configRegion.entrySet()) {
          Configuration config = stringConfigurationEntry.getValue();
          for (String jar : config.getJarNames()) {
            if (!getPathToJarOnThisLocator(stringConfigurationEntry.getKey(), jar).toFile()
                .exists()) {
              downloadJarFromOtherLocators(stringConfigurationEntry.getKey(), jar);
            }
          }
        }
      }
    } finally {
      unlockSharedConfiguration();
    }

    status.set(SharedConfigurationStatus.RUNNING);
  }

  void removeInvalidXmlConfigurations(Region<String, Configuration> configRegion)
      throws IOException {
    for (Map.Entry<String, Configuration> entry : configRegion.entrySet()) {
      String group = entry.getKey();
      Configuration configuration = entry.getValue();
      String configurationXml = configuration.getCacheXmlContent();
      if (configurationXml != null && !configurationXml.isEmpty()) {
        try {
          Document document = XmlUtils.createDocumentFromXml(configurationXml);
          boolean removedInvalidReceivers = removeInvalidGatewayReceivers(document);
          boolean removedDuplicateReceivers = removeDuplicateGatewayReceivers(document);
          if (removedInvalidReceivers || removedDuplicateReceivers) {
            configuration.setCacheXmlContent(XmlUtils.prettyXml(document));
            configRegion.put(group, configuration);
          }
        } catch (SAXException | TransformerException | ParserConfigurationException e) {
          throw new IOException("Unable to parse existing cluster configuration from disk. ", e);
        }
      }
    }
  }

  boolean removeInvalidGatewayReceivers(Document document) throws TransformerException {
    boolean modified = false;
    NodeList receiverNodes = document.getElementsByTagName("gateway-receiver");
    for (int i = receiverNodes.getLength() - 1; i >= 0; i--) {
      Element receiverElement = (Element) receiverNodes.item(i);

      // Check hostname-for-senders
      String hostNameForSenders = receiverElement.getAttribute("hostname-for-senders");
      if (StringUtils.isNotBlank(hostNameForSenders)) {
        receiverElement.getParentNode().removeChild(receiverElement);
        logger.info("Removed invalid cluster configuration gateway-receiver element="
            + XmlUtils.prettyXml(receiverElement));
        modified = true;
      }

      // Check bind-address
      String bindAddress = receiverElement.getAttribute("bind-address");
      if (StringUtils.isNotBlank(bindAddress) && !bindAddress.equals("0.0.0.0")) {
        receiverElement.getParentNode().removeChild(receiverElement);
        logger.info("Removed invalid cluster configuration gateway-receiver element="
            + XmlUtils.prettyXml(receiverElement));
        modified = true;
      }
    }
    return modified;
  }

  boolean removeDuplicateGatewayReceivers(Document document) throws TransformerException {
    boolean modified = false;
    NodeList receiverNodes = document.getElementsByTagName("gateway-receiver");
    while (receiverNodes.getLength() > 1) {
      Element receiverElement = (Element) receiverNodes.item(0);
      receiverElement.getParentNode().removeChild(receiverElement);
      logger.info("Removed duplicate cluster configuration gateway-receiver element="
          + XmlUtils.prettyXml(receiverElement));
      modified = true;
      receiverNodes = document.getElementsByTagName("gateway-receiver");
    }
    return modified;
  }

  private void persistSecuritySettings(final Region<String, Configuration> configRegion) {
    Properties securityProps = cache.getDistributedSystem().getSecurityProperties();

    Configuration clusterPropertiesConfig =
        configRegion.get(ConfigurationPersistenceService.CLUSTER_CONFIG);
    if (clusterPropertiesConfig == null) {
      clusterPropertiesConfig = new Configuration(ConfigurationPersistenceService.CLUSTER_CONFIG);
      configRegion.put(ConfigurationPersistenceService.CLUSTER_CONFIG, clusterPropertiesConfig);
    }
    // put security-manager and security-post-processor in the cluster config
    Properties clusterProperties = clusterPropertiesConfig.getGemfireProperties();

    if (securityProps.containsKey(SECURITY_MANAGER)) {
      clusterProperties.setProperty(SECURITY_MANAGER, securityProps.getProperty(SECURITY_MANAGER));
    }
    if (securityProps.containsKey(SECURITY_POST_PROCESSOR)) {
      clusterProperties.setProperty(SECURITY_POST_PROCESSOR,
          securityProps.getProperty(SECURITY_POST_PROCESSOR));
    }
  }

  /**
   * Creates a ConfigurationResponse based on the configRequest, configuration response contains the
   * requested shared configuration This method locks the ConfigurationPersistenceService
   */
  public ConfigurationResponse createConfigurationResponse(Set<String> groups) {
    ConfigurationResponse configResponse = null;

    boolean isLocked = lockSharedConfiguration();
    if (isLocked) {
      try {
        configResponse = new ConfigurationResponse();
        groups.add(ConfigurationPersistenceService.CLUSTER_CONFIG);
        logger.info("Building up configuration response with following configurations: {}", groups);

        for (String group : groups) {
          Configuration configuration = getConfiguration(group);
          configResponse.addConfiguration(configuration);
          if (configuration != null) {
            configResponse.addJar(group, configuration.getJarNames());
          }
        }
        return configResponse;

      } finally {
        unlockSharedConfiguration();
      }
    }
    return configResponse;
  }

  /**
   * Create a response containing the status of the Shared configuration and information about other
   * locators containing newer shared configuration data (if at all)
   *
   * @return {@link SharedConfigurationStatusResponse} containing the
   *         {@link SharedConfigurationStatus}
   */
  SharedConfigurationStatusResponse createStatusResponse() {
    SharedConfigurationStatusResponse response = new SharedConfigurationStatusResponse();
    response.setStatus(getStatus());
    response.addWaitingLocatorInfo(newerSharedConfigurationLocatorInfo);
    return response;
  }

  /**
   * For tests only. TODO: clean this up and remove from production code
   * <p>
   * Throws {@code AssertionError} wrapping any exception thrown by operation.
   */
  public void destroySharedConfiguration() {
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      if (configRegion != null) {
        configRegion.destroyRegion();
      }
      DiskStore configDiskStore = cache.findDiskStore(CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
      if (configDiskStore != null) {
        configDiskStore.destroy();
      }
      FileUtils.deleteDirectory(configDirPath.toFile());
    } catch (Exception exception) {
      throw new AssertionError(exception);
    }
  }

  public Path getPathToJarOnThisLocator(String groupName, String jarName) {
    return configDirPath.resolve(groupName).resolve(jarName);
  }

  public Configuration getConfiguration(String groupName) {
    return getConfigurationRegion().get(groupName);
  }

  public void setConfiguration(String groupName, Configuration configuration) {
    getConfigurationRegion().put(groupName, configuration);
  }

  public boolean hasXmlConfiguration() {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    return configRegion.values().stream().anyMatch(c -> c.getCacheXmlContent() != null);
  }

  public Map<String, Configuration> getEntireConfiguration() {
    Set<String> keys = getConfigurationRegion().keySet();
    return getConfigurationRegion().getAll(keys);
  }

  public Set<String> getGroups() {
    return getConfigurationRegion().keySet();
  }

  public Path getClusterConfigDirPath() {
    return configDirPath;
  }

  /**
   * Gets the current status of the ConfigurationPersistenceService If the status is started , it
   * determines if the shared configuration is waiting for new configuration on other locators
   *
   * @return {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatus getStatus() {
    SharedConfigurationStatus scStatus = status.get();
    if (scStatus == SharedConfigurationStatus.STARTED) {
      PersistentMemberManager pmm = cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = pmm.getWaitingRegions();
      if (!waitingRegions.isEmpty()) {
        status.compareAndSet(SharedConfigurationStatus.STARTED,
            SharedConfigurationStatus.WAITING);
        Set<PersistentMemberID> persMemIds =
            waitingRegions.get(Region.SEPARATOR_CHAR + CONFIG_REGION_NAME);
        for (PersistentMemberID persMemId : persMemIds) {
          newerSharedConfigurationLocatorInfo.add(new PersistentMemberPattern(persMemId));
        }
      }
    }
    return status.get();
  }

  // configDir is the dir that has all the groups structure underneath it.
  public void loadSharedConfigurationFromDir(File configDir) throws IOException {
    lockSharedConfiguration();
    try {
      File[] groupNames = configDir.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
      boolean needToCopyJars = true;
      if (configDir.getAbsolutePath().equals(configDirPath.toAbsolutePath().toString())) {
        needToCopyJars = false;
      }

      logger.info("loading the cluster configuration: ");
      Map<String, Configuration> sharedConfiguration = new HashMap<>();
      for (File groupName : groupNames) {
        Configuration configuration = readConfiguration(groupName);
        logger.info(configuration.getConfigName() + " xml content: \n"
            + configuration.getCacheXmlContent());
        logger.info(configuration.getConfigName() + " properties: "
            + configuration.getGemfireProperties().size());
        logger.info(configuration.getConfigName() + " jars: "
            + Strings.join(configuration.getJarNames(), ", "));
        sharedConfiguration.put(groupName.getName(), configuration);
        if (needToCopyJars && configuration.getJarNames().size() > 0) {
          Path groupDirPath = createConfigDirIfNecessary(configuration.getConfigName()).toPath();
          for (String jarName : configuration.getJarNames()) {
            Files.copy(groupName.toPath().resolve(jarName), groupDirPath.resolve(jarName));
          }
        }
      }
      Region<String, Configuration> clusterRegion = getConfigurationRegion();
      clusterRegion.clear();

      String memberId = cache.getMyId().getId();
      clusterRegion.putAll(sharedConfiguration, memberId);

      // Overwrite the security settings using the locator's properties, ignoring whatever
      // in the import
      persistSecuritySettings(clusterRegion);
    } finally {
      unlockSharedConfiguration();
    }
  }

  // Write the content of xml and properties into the file system for exporting purpose
  public void writeConfigToFile(final Configuration configuration, File rootDir)
      throws IOException {
    File configDir = createConfigDirIfNecessary(rootDir, configuration.getConfigName());

    File propsFile = new File(configDir, configuration.getPropertiesFileName());
    BufferedWriter bw = new BufferedWriter(new FileWriter(propsFile));
    configuration.getGemfireProperties().store(bw, null);
    bw.close();

    File xmlFile = new File(configDir, configuration.getCacheXmlFileName());
    FileUtils.writeStringToFile(xmlFile, configuration.getCacheXmlContent(), "UTF-8");

    // copy the jars if the rootDir is different than the configDirPath
    if (rootDir.getAbsolutePath().equals(configDirPath.toAbsolutePath().toString())) {
      return;
    }

    File locatorConfigDir = configDirPath.resolve(configuration.getConfigName()).toFile();
    if (locatorConfigDir.exists()) {
      File[] jarFiles = locatorConfigDir.listFiles(x -> x.getName().endsWith(".jar"));
      for (File file : jarFiles) {
        Files.copy(file.toPath(), configDir.toPath().resolve(file.getName()));
      }
    }
  }

  public boolean lockSharedConfiguration() {
    return sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, -1, -1);
  }

  public void unlockSharedConfiguration() {
    sharedConfigLockingService.unlock(SHARED_CONFIG_LOCK_NAME);
  }

  /**
   * Gets the region containing the shared configuration data. The region is created , if it does
   * not exist already. Note : this could block if this locator contains stale persistent
   * configuration data.
   *
   * @return {@link Region} ConfigurationRegion, this should never be null
   */
  public Region<String, Configuration> getConfigurationRegion() {
    Region<String, Configuration> configRegion = cache.getRegion(CONFIG_REGION_NAME);
    if (configRegion != null) {
      return configRegion;
    }

    try {
      File diskDir = configDiskDirPath.toFile();

      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory at " + configDiskDirPath);
      }

      File[] diskDirs = {diskDir};
      cache.createDiskStoreFactory().setDiskDirs(diskDirs).setAutoCompact(true)
          .setMaxOplogSize(10).create(CLUSTER_CONFIG_DISK_STORE_NAME);

      InternalRegionFactory<String, Configuration> regionFactory =
          cache.createInternalRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
      regionFactory.addCacheListener(new ConfigurationChangeListener(this, cache));
      regionFactory.setDiskStoreName(CLUSTER_CONFIG_DISK_STORE_NAME);
      regionFactory.setIsUsedForMetaRegion(true).setMetaRegionWithTransactions(false);
      return regionFactory.create(CONFIG_REGION_NAME);
    } catch (RuntimeException e) {
      status.set(SharedConfigurationStatus.STOPPED);
      // throw RuntimeException as is
      throw e;
    } catch (Exception e) {
      status.set(SharedConfigurationStatus.STOPPED);
      // turn all other exceptions into runtime exceptions
      throw new RuntimeException("Error occurred while initializing cluster configuration", e);
    }
  }

  /**
   * Reads the configuration information from the shared configuration directory and returns a
   * {@link Configuration} object
   *
   * @return {@link Configuration}
   */
  private Configuration readConfiguration(File groupConfigDir) throws IOException {
    Configuration configuration = new Configuration(groupConfigDir.getName());
    File cacheXmlFull = new File(groupConfigDir, configuration.getCacheXmlFileName());
    File propertiesFull = new File(groupConfigDir, configuration.getPropertiesFileName());

    configuration.setCacheXmlFile(cacheXmlFull);
    configuration.setPropertiesFile(propertiesFull);

    String deployedBy = getDeployedBy();
    String deployedTime = Instant.now().toString();
    List<String> fileNames = asList(groupConfigDir.list());
    loadDeploymentsFromFileNames(fileNames, configuration, deployedBy, deployedTime);
    return configuration;
  }

  private String getDeployedBy() {
    Subject subject = cache.getSecurityService().getSubject();
    return subject == null ? null : subject.getPrincipal().toString();
  }

  @VisibleForTesting
  static void loadDeploymentsFromFileNames(Collection<String> fileNames,
      Configuration configuration, String deployedBy, String deployedTime) {
    fileNames.stream()
        .filter(filename -> filename.endsWith(".jar"))
        .map(jarFileName -> new Deployment(jarFileName, deployedBy, deployedTime))
        .forEach(configuration::putDeployment);
  }

  /**
   * Creates a directory for this configuration if it doesn't already exist.
   */
  private File createConfigDirIfNecessary(final String configName) throws IOException {
    return createConfigDirIfNecessary(configDirPath.toFile(), configName);
  }

  private File createConfigDirIfNecessary(File clusterConfigDir, final String configName)
      throws IOException {
    if (!clusterConfigDir.exists() && !clusterConfigDir.mkdirs()) {
      throw new IOException("Cannot create directory : " + configDirPath);
    }
    Path configDirPath = clusterConfigDir.toPath().resolve(configName);

    File configDir = configDirPath.toFile();
    if (!configDir.exists() && !configDir.mkdir()) {
      throw new IOException("Cannot create directory : " + configDirPath);
    }

    return configDir;
  }

  private String generateInitialXmlContent() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    CacheXmlGenerator.generateDefault(pw);
    return sw.toString();
  }

  @Override
  public CacheConfig getCacheConfig(String group) {
    return getCacheConfig(group, false);
  }


  @Override
  public CacheConfig getCacheConfig(String group, boolean createNew) {
    if (group == null) {
      group = CLUSTER_CONFIG;
    }
    Configuration configuration = getConfiguration(group);
    if (configuration == null) {
      if (createNew) {
        return new CacheConfig(CACHE_CONFIG_VERSION);
      }
      return null;
    }
    String xmlContent = configuration.getCacheXmlContent();
    // group existed, so we should create a blank one to start with
    if (xmlContent == null || xmlContent.isEmpty()) {
      if (createNew) {
        return new CacheConfig(CACHE_CONFIG_VERSION);
      }
      return null;
    }

    return jaxbService.unMarshall(xmlContent);
  }

  @Override
  public void updateCacheConfig(String group, UnaryOperator<CacheConfig> mutator) {
    if (group == null) {
      group = CLUSTER_CONFIG;
    }
    lockSharedConfiguration();
    try {
      CacheConfig cacheConfig = getCacheConfig(group, true);
      cacheConfig = mutator.apply(cacheConfig);
      if (cacheConfig == null) {
        // mutator returns a null config, indicating no change needs to be persisted
        return;
      }
      Configuration configuration = getConfiguration(group);
      if (configuration == null) {
        configuration = new Configuration(group);
      }
      configuration.setCacheXmlContent(jaxbService.marshall(cacheConfig));
      getConfigurationRegion().put(group, configuration);
    } finally {
      unlockSharedConfiguration();
    }
  }
}
