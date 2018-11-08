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

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import joptsimple.internal.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.CancelException;
import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.XSDRootElement;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;
import org.apache.geode.management.internal.configuration.callbacks.ConfigurationChangeListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.functions.DownloadJarFunction;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;

@SuppressWarnings({"deprecation", "unchecked"})
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

  private final String configDirPath;
  private final String configDiskDirPath;

  private final Set<PersistentMemberPattern> newerSharedConfigurationLocatorInfo = new HashSet<>();
  private final AtomicReference<SharedConfigurationStatus> status = new AtomicReference<>();

  private final InternalCache cache;
  private DistributedLockService sharedConfigLockingService;
  private JAXBService jaxbService;

  @TestingOnly
  InternalConfigurationPersistenceService(Class<?>... xsdClasses) {
    configDirPath = null;
    configDiskDirPath = null;
    cache = null;
    jaxbService = new JAXBService(xsdClasses);
    jaxbService.validateWithLocalCacheXSD();
  }

  @TestingOnly
  InternalConfigurationPersistenceService() {
    configDirPath = null;
    configDiskDirPath = null;
    cache = null;
    jaxbService = new JAXBService(CacheConfig.class);
  }

  public InternalConfigurationPersistenceService(InternalCache cache, Class<?>... xsdClasses)
      throws IOException {
    this.cache = cache;
    Properties properties = cache.getDistributedSystem().getProperties();
    // resolve the cluster config dir
    String clusterConfigRootDir = properties.getProperty(CLUSTER_CONFIGURATION_DIR);

    if (StringUtils.isBlank(clusterConfigRootDir)) {
      clusterConfigRootDir = System.getProperty("user.dir");
    } else {
      File diskDir = new File(clusterConfigRootDir);
      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory : " + clusterConfigRootDir);
      }
      clusterConfigRootDir = diskDir.getCanonicalPath();
    }
    clusterConfigRootDir = new File(clusterConfigRootDir).getAbsolutePath();

    // resolve the file paths
    String configDiskDirName =
        CLUSTER_CONFIG_DISK_DIR_PREFIX + cache.getDistributedSystem().getName();

    this.configDirPath =
        FilenameUtils.concat(clusterConfigRootDir, CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
    this.configDiskDirPath = FilenameUtils.concat(clusterConfigRootDir, configDiskDirName);
    this.sharedConfigLockingService = getSharedConfigLockService(cache.getDistributedSystem());
    this.status.set(SharedConfigurationStatus.NOT_STARTED);
    if (xsdClasses != null && xsdClasses.length > 0) {
      this.jaxbService = new JAXBService(xsdClasses);
    }
    // else, scan the classpath to find all the classes annotated with XSDRootElement
    else {
      Set<String> packages = getPackagesToScan();
      ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packages);
      Set<Class<?>> scannedClasses = scanner.scanClasspathForAnnotation(XSDRootElement.class,
          packages.toArray(new String[] {}));
      this.jaxbService = new JAXBService(scannedClasses.toArray(new Class[scannedClasses.size()]));
    }
    jaxbService.validateWithLocalCacheXSD();
  }

  protected Set<String> getPackagesToScan() {
    Set<String> packages = new HashSet<>();
    String sysProperty = SystemPropertyHelper.getProperty(SystemPropertyHelper.PACKAGES_TO_SCAN);
    if (sysProperty != null) {
      packages = Arrays.stream(sysProperty.split(",")).collect(Collectors.toSet());
    } else {
      packages.add("*");
    }
    return packages;
  }

  /**
   * Gets or creates (if not created) shared configuration lock service
   */
  private DistributedLockService getSharedConfigLockService(DistributedSystem ds) {
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

  /**
   * Adds/replaces the xml entity in the shared configuration we don't need to trigger the change
   * listener for this modification, so it's ok to operate on the original configuration object
   */
  public void addXmlEntity(XmlEntity xmlEntity, String[] groups) {
    lockSharedConfiguration();
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      if (groups == null || groups.length == 0) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      for (String group : groups) {
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
        groups = groupSet.toArray(new String[groupSet.size()]);
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
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : groups) {
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
    lockSharedConfiguration();
    try {
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : groups) {
        Configuration configuration = configRegion.get(group);

        if (configuration == null) {
          configuration = new Configuration(group);
          createConfigDirIfNecessary(group);
        }

        String groupDir = FilenameUtils.concat(this.configDirPath, group);
        Set<String> jarNames = new HashSet<>();
        for (String jarFullPath : jarFullPaths) {
          File stagedJar = new File(jarFullPath);
          jarNames.add(stagedJar.getName());
          String filePath = FilenameUtils.concat(groupDir, stagedJar.getName());
          File jarFile = new File(filePath);
          FileUtils.copyFile(stagedJar, jarFile);
        }

        // update the record after writing the jars to the file system, since the listener
        // will need the jars on file to upload to other locators. Need to update the jars
        // using a new copy of the Configuration so that the change listener will pick up the jar
        // name changes.
        String memberId = cache.getMyId().getId();

        Configuration configurationCopy = new Configuration(configuration);
        configurationCopy.addJarNames(jarNames);
        configRegion.put(group, configurationCopy, memberId);
      }
    } finally {
      unlockSharedConfiguration();
    }
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
          File jar = this.getPathToJarOnThisLocator(group, jarRemoved).toFile();
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
    DistributionManager dm = this.cache.getDistributionManager();
    DistributedMember me = this.cache.getMyId();
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
    Files.copy(jarFile.toPath(), jarToWrite.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
    ResultCollector<RemoteInputStream, List<RemoteInputStream>> rc =
        (ResultCollector<RemoteInputStream, List<RemoteInputStream>>) CliUtil.executeFunction(
            new DownloadJarFunction(), new Object[] {groupName, jarName},
            Collections.singleton(locator));

    List<RemoteInputStream> result = rc.getResult();
    RemoteInputStream jarStream = result.get(0);

    Path tempDir = FileUploader.createSecuredTempDirectory("deploy-");
    Path tempJar = Paths.get(tempDir.toString(), jarName);
    FileOutputStream fos = new FileOutputStream(tempJar.toString());
    InputStream input = RemoteInputStreamClient.wrap(jarStream);

    IOUtils.copy(input, fos);

    fos.close();
    input.close();

    return tempJar.toFile();
  }

  /**
   * Creates the shared configuration service
   *
   * @param loadSharedConfigFromDir when set to true, loads the configuration from the share_config
   *        directory
   */
  void initSharedConfiguration(boolean loadSharedConfigFromDir)
      throws CacheLoaderException, TimeoutException, IllegalStateException, IOException,
      TransformerException, SAXException, ParserConfigurationException {
    this.status.set(SharedConfigurationStatus.STARTED);
    Region<String, Configuration> configRegion = this.getConfigurationRegion();
    lockSharedConfiguration();
    removeInvalidXmlConfigurations(configRegion);
    try {
      if (loadSharedConfigFromDir) {
        logger.info("Reading cluster configuration from '{}' directory",
            InternalConfigurationPersistenceService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
        loadSharedConfigurationFromDir(new File(this.configDirPath));
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

    this.status.set(SharedConfigurationStatus.RUNNING);
  }

  void removeInvalidXmlConfigurations(Region<String, Configuration> configRegion)
      throws IOException, SAXException, ParserConfigurationException, TransformerException {
    for (Map.Entry<String, Configuration> entry : configRegion.entrySet()) {
      String group = entry.getKey();
      Configuration configuration = entry.getValue();
      String configurationXml = configuration.getCacheXmlContent();
      if (configurationXml != null && !configurationXml.isEmpty()) {
        Document document = XmlUtils.createDocumentFromXml(configurationXml);
        boolean removedInvalidReceivers = removeInvalidGatewayReceivers(document);
        boolean removedDuplicateReceivers = removeDuplicateGatewayReceivers(document);
        if (removedInvalidReceivers || removedDuplicateReceivers) {
          configuration.setCacheXmlContent(XmlUtils.prettyXml(document));
          configRegion.put(group, configuration);
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
    Properties securityProps = this.cache.getDistributedSystem().getSecurityProperties();

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
    try {
      if (isLocked) {
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
      }
    } finally {
      unlockSharedConfiguration();
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
    response.addWaitingLocatorInfo(this.newerSharedConfigurationLocatorInfo);
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
      DiskStore configDiskStore = this.cache.findDiskStore(CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
      if (configDiskStore != null) {
        configDiskStore.destroy();
        File file = new File(this.configDiskDirPath);
        FileUtils.deleteDirectory(file);
      }
      FileUtils.deleteDirectory(new File(this.configDirPath));
    } catch (Exception exception) {
      throw new AssertionError(exception);
    }
  }

  public Path getPathToJarOnThisLocator(String groupName, String jarName) {
    return new File(this.configDirPath).toPath().resolve(groupName).resolve(jarName);
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

  /**
   * Returns the path of Shared configuration directory
   *
   * @return {@link String} path of the shared configuration directory
   */
  public String getSharedConfigurationDirPath() {
    return configDirPath;
  }

  public Path getClusterConfigDirPath() {
    return Paths.get(configDirPath);
  }

  /**
   * Gets the current status of the ConfigurationPersistenceService If the status is started , it
   * determines if the shared configuration is waiting for new configuration on other locators
   *
   * @return {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatus getStatus() {
    SharedConfigurationStatus scStatus = this.status.get();
    if (scStatus == SharedConfigurationStatus.STARTED) {
      PersistentMemberManager pmm = this.cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = pmm.getWaitingRegions();
      if (!waitingRegions.isEmpty()) {
        this.status.compareAndSet(SharedConfigurationStatus.STARTED,
            SharedConfigurationStatus.WAITING);
        Set<PersistentMemberID> persMemIds =
            waitingRegions.get(Region.SEPARATOR_CHAR + CONFIG_REGION_NAME);
        for (PersistentMemberID persMemId : persMemIds) {
          this.newerSharedConfigurationLocatorInfo.add(new PersistentMemberPattern(persMemId));
        }
      }
    }
    return this.status.get();
  }

  // configDir is the dir that has all the groups structure underneath it.
  public void loadSharedConfigurationFromDir(File configDir)
      throws SAXException, ParserConfigurationException, TransformerException, IOException {
    lockSharedConfiguration();
    try {
      File[] groupNames = configDir.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
      boolean needToCopyJars = true;
      if (configDir.getAbsolutePath().equals(getSharedConfigurationDirPath())) {
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
    if (rootDir.getAbsolutePath().equals(getSharedConfigurationDirPath())) {
      return;
    }

    File locatorConfigDir =
        new File(getSharedConfigurationDirPath(), configuration.getConfigName());
    if (locatorConfigDir.exists()) {
      File[] jarFiles = locatorConfigDir.listFiles(x -> x.getName().endsWith(".jar"));
      for (File file : jarFiles) {
        Files.copy(file.toPath(), configDir.toPath().resolve(file.getName()));
      }
    }
  }

  public boolean lockSharedConfiguration() {
    return this.sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, -1, -1);
  }

  public void unlockSharedConfiguration() {
    this.sharedConfigLockingService.unlock(SHARED_CONFIG_LOCK_NAME);
  }

  /**
   * Gets the region containing the shared configuration data. The region is created , if it does
   * not exist already. Note : this could block if this locator contains stale persistent
   * configuration data.
   *
   * @return {@link Region} ConfigurationRegion, this should never be null
   */
  public Region<String, Configuration> getConfigurationRegion() {
    Region<String, Configuration> configRegion = this.cache.getRegion(CONFIG_REGION_NAME);

    try {
      if (configRegion == null) {
        File diskDir = new File(this.configDiskDirPath);

        if (!diskDir.exists()) {
          if (!diskDir.mkdirs()) {
            // TODO: throw caught by containing try statement
            throw new IOException("Cannot create directory at " + this.configDiskDirPath);
          }
        }

        File[] diskDirs = {diskDir};
        this.cache.createDiskStoreFactory().setDiskDirs(diskDirs).setAutoCompact(true)
            .setMaxOplogSize(10).create(CLUSTER_CONFIG_DISK_STORE_NAME);

        AttributesFactory<String, Configuration> regionAttrsFactory = new AttributesFactory<>();
        regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionAttrsFactory.setCacheListener(new ConfigurationChangeListener(this, cache));
        regionAttrsFactory.setDiskStoreName(CLUSTER_CONFIG_DISK_STORE_NAME);
        regionAttrsFactory.setScope(Scope.DISTRIBUTED_ACK);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        internalArgs.setMetaRegionWithTransactions(false);

        configRegion = this.cache.createVMRegion(CONFIG_REGION_NAME, regionAttrsFactory.create(),
            internalArgs);
      }

    } catch (CancelException e) {
      if (configRegion == null) {
        this.status.set(SharedConfigurationStatus.STOPPED);
      }
      // CONFIG: don't rethrow as Exception, keep it a subclass of CancelException
      throw e;

    } catch (Exception e) {
      if (configRegion == null) {
        this.status.set(SharedConfigurationStatus.STOPPED);
      }
      throw new RuntimeException("Error occurred while initializing cluster configuration", e);
    }

    return configRegion;
  }

  /**
   * Reads the configuration information from the shared configuration directory and returns a
   * {@link Configuration} object
   *
   * @return {@link Configuration}
   */
  private Configuration readConfiguration(File groupConfigDir)
      throws SAXException, ParserConfigurationException, TransformerFactoryConfigurationError,
      TransformerException, IOException {
    Configuration configuration = new Configuration(groupConfigDir.getName());
    File cacheXmlFull = new File(groupConfigDir, configuration.getCacheXmlFileName());
    File propertiesFull = new File(groupConfigDir, configuration.getPropertiesFileName());

    configuration.setCacheXmlFile(cacheXmlFull);
    configuration.setPropertiesFile(propertiesFull);

    Set<String> jarFileNames = Arrays.stream(groupConfigDir.list())
        .filter((String filename) -> filename.endsWith(".jar")).collect(Collectors.toSet());
    configuration.addJarNames(jarFileNames);
    return configuration;
  }

  /**
   * Creates a directory for this configuration if it doesn't already exist.
   */
  private File createConfigDirIfNecessary(final String configName) throws IOException {
    return createConfigDirIfNecessary(new File(getSharedConfigurationDirPath()), configName);
  }

  private File createConfigDirIfNecessary(File clusterConfigDir, final String configName)
      throws IOException {
    if (!clusterConfigDir.exists()) {
      if (!clusterConfigDir.mkdirs()) {
        throw new IOException("Cannot create directory : " + getSharedConfigurationDirPath());
      }
    }
    Path configDirPath = clusterConfigDir.toPath().resolve(configName);

    File configDir = configDirPath.toFile();
    if (!configDir.exists()) {
      if (!configDir.mkdir()) {
        throw new IOException("Cannot create directory : " + configDirPath);
      }
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
    if (group == null) {
      group = CLUSTER_CONFIG;
    }
    Configuration configuration = getConfiguration(group);
    if (configuration == null) {
      return null;
    }
    String xmlContent = configuration.getCacheXmlContent();
    // group existed, so we should create a blank one to start with
    if (xmlContent == null || xmlContent.isEmpty()) {
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
      CacheConfig cacheConfig = getCacheConfig(group);
      if (cacheConfig == null) {
        cacheConfig = new CacheConfig("1.0");
      }
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
