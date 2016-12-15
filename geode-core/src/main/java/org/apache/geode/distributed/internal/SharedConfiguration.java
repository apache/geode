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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.geode.CancelException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.configuration.callbacks.ConfigurationChangeListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.functions.UploadJarFunction;
import org.apache.geode.management.internal.configuration.messages.ConfigurationRequest;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPathExpressionException;

@SuppressWarnings({"deprecation", "unchecked"})
public class SharedConfiguration {

  private static final Logger logger = LogService.getLogger();

  /**
   * Name of the directory where the shared configuration artifacts are stored
   */
  public static final String CLUSTER_CONFIG_ARTIFACTS_DIR_NAME = "cluster_config";
  private static final String CLUSTER_CONFIG_DISK_STORE_NAME = "cluster_config";
  public static final String CLUSTER_CONFIG_DISK_DIR_PREFIX = "ConfigDiskDir_";

  public static final String CLUSTER_CONFIG = "cluster";

  /**
   * Name of the lock service used for shared configuration
   */
  private static final String SHARED_CONFIG_LOCK_SERVICE_NAME = "__CLUSTER_CONFIG_LS";

  /**
   * Name of the lock for locking the shared configuration
   */
  public static final String SHARED_CONFIG_LOCK_NAME = "__CLUSTER_CONFIG_LOCK";

  /**
   * Name of the region which is used to store the configuration information
   */
  private static final String CONFIG_REGION_NAME = "_ConfigurationRegion";

  private final String configDirPath;
  private final String configDiskDirName;
  private final String configDiskDirPath;

  private final Set<PersistentMemberPattern> newerSharedConfigurationLocatorInfo =
      new HashSet<PersistentMemberPattern>();
  private final AtomicReference<SharedConfigurationStatus> status =
      new AtomicReference<SharedConfigurationStatus>();
  private static final JarFileFilter jarFileFilter = new JarFileFilter();

  private GemFireCacheImpl cache;
  private final DistributedLockService sharedConfigLockingService;

  /**
   * Gets or creates (if not created) shared configuration lock service
   */
  public static DistributedLockService getSharedConfigLockService(DistributedSystem ds) {
    DistributedLockService sharedConfigDls =
        DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    try {
      if (sharedConfigDls == null) {
        sharedConfigDls = DLockService.create(SHARED_CONFIG_LOCK_SERVICE_NAME,
            (InternalDistributedSystem) ds, true, true);
      }
    } catch (IllegalArgumentException e) {
      return DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    }
    return sharedConfigDls;
  }

  public SharedConfiguration(Cache cache) throws IOException {
    this.cache = (GemFireCacheImpl) cache;
    this.configDiskDirName =
        CLUSTER_CONFIG_DISK_DIR_PREFIX + cache.getDistributedSystem().getName();
    String clusterConfigDir =
        cache.getDistributedSystem().getProperties().getProperty(CLUSTER_CONFIGURATION_DIR);
    if (StringUtils.isBlank(clusterConfigDir)) {
      clusterConfigDir = System.getProperty("user.dir");
    } else {
      File diskDir = new File(clusterConfigDir);
      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory : " + clusterConfigDir);
      }
      clusterConfigDir = diskDir.getCanonicalPath();
    }
    this.configDiskDirPath = FilenameUtils.concat(clusterConfigDir, this.configDiskDirName);
    configDirPath = FilenameUtils.concat(clusterConfigDir, CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
    sharedConfigLockingService = getSharedConfigLockService(cache.getDistributedSystem());
    status.set(SharedConfigurationStatus.NOT_STARTED);
  }

  /**
   * Add jar information into the shared configuration and save the jars in the file system
   * @return true on success
   */
  public boolean addJarsToThisLocator(String[] jarNames, byte[][] jarBytes, String[] groups) {
    boolean success = true;
    try {
      if (groups == null) {
        groups = new String[]{SharedConfiguration.CLUSTER_CONFIG};
      }
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : groups) {
        Configuration configuration = configRegion.get(group);

        if (configuration == null) {
          configuration = new Configuration(group);
          createConfigDirIfNecessary(group);
        }
        String groupDir = FilenameUtils.concat(configDirPath, group);
        writeJarFiles(groupDir, jarNames, jarBytes);

        // update the record after writing the jars to the file system, since the listener
        // will need the jars on file to upload to other locators.
        configuration.addJarNames(jarNames);
        configRegion.put(group, configuration);
      }
    } catch (Exception e) {
      success = false;
      logger.info(e.getMessage(), e);
    }
    return success;
  }

  /**
   * Adds/replaces the xml entity in the shared configuration
   */
  public void addXmlEntity(XmlEntity xmlEntity, String[] groups) throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    if (groups == null || groups.length == 0) {
      groups = new String[]{SharedConfiguration.CLUSTER_CONFIG};
    }
    for (String group : groups) {
      Configuration configuration = (Configuration) configRegion.get(group);
      if (configuration == null) {
        configuration = new Configuration(group);
      }
      String xmlContent = configuration.getCacheXmlContent();
      if (xmlContent == null || xmlContent.isEmpty()) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        CacheXmlGenerator.generateDefault(pw);
        xmlContent = sw.toString();
      }
      final Document doc = createAndUpgradeDocumentFromXml(xmlContent);
      XmlUtils.addNewNode(doc, xmlEntity);
      configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
      configRegion.put(group, configuration);
//      writeConfig(configuration);
    }
  }

  public void clearSharedConfiguration() throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    if (configRegion != null) {
      configRegion.clear();
    }
  }

  /**
   * Creates the shared configuration service
   * @param loadSharedConfigFromDir when set to true, loads the configuration from the share_config
   * directory
   */
  public void initSharedConfiguration(boolean loadSharedConfigFromDir) throws Exception {
    status.set(SharedConfigurationStatus.STARTED);
    Region<String, Configuration> configRegion = this.getConfigurationRegion();

    if (loadSharedConfigFromDir) {
      lockSharedConfiguration();
      try {
        logger.info("Reading cluster configuration from '{}' directory",
            SharedConfiguration.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);

        Map<String, Configuration> sharedConfigMap = this.readSharedConfigurationFromDisk();
        // Clear the configuration region and load the configuration read from the 'shared_config'
        // directory
        // on region entry create/update, it will upload the jars to all other locators
        configRegion.clear();
        configRegion.putAll(sharedConfigMap);
      } finally {
        unlockSharedConfiguration();
      }
    } else {
      // Write out the existing configuration into the 'shared_config' directory
      // And get deployed jars from other locators.
      lockSharedConfiguration();
      try {
        // on region entry create/update, it should download missing jars from other locators
        putSecurityPropsIntoClusterConfig(configRegion);
      } finally {
        unlockSharedConfiguration();
      }
    }

    status.set(SharedConfigurationStatus.RUNNING);
  }

  private void putSecurityPropsIntoClusterConfig(final Region<String, Configuration> configRegion) {
    Properties securityProps = cache.getDistributedSystem().getSecurityProperties();
    Configuration clusterPropertiesConfig = configRegion.get(SharedConfiguration.CLUSTER_CONFIG);
    if (clusterPropertiesConfig == null) {
      clusterPropertiesConfig = new Configuration(SharedConfiguration.CLUSTER_CONFIG);
      configRegion.put(SharedConfiguration.CLUSTER_CONFIG, clusterPropertiesConfig);
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
   * requested shared configuration This method locks the SharedConfiguration
   */
  public ConfigurationResponse createConfigurationReponse(final ConfigurationRequest configRequest)
      throws Exception {

    ConfigurationResponse configResponse = new ConfigurationResponse();

    for (int i = 0; i < configRequest.getNumAttempts(); i++) {
      boolean isLocked = sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, 5000, 5000);
      try {
        if (isLocked) {
          Set<String> groups = configRequest.getGroups();
          groups.add(SharedConfiguration.CLUSTER_CONFIG);
          logger.info("Building up configuration response with following configurations: {}",
              groups);

          for (String group : groups) {
            Configuration configuration = getConfiguration(group);
            configResponse.addConfiguration(configuration);
          }

          Map<String, byte[]> jarNamesToJarBytes = getAllJarsFromThisLocator(groups);
          String[] jarNames = jarNamesToJarBytes.keySet().stream().toArray(String[]::new);
          byte[][] jarBytes = jarNamesToJarBytes.values().toArray(new byte[jarNames.length][]);

          configResponse.addJarsToBeDeployed(jarNames, jarBytes);
          configResponse.setFailedToGetSharedConfig(false);
          return configResponse;
        }
      } finally {
        sharedConfigLockingService.unlock(SHARED_CONFIG_LOCK_NAME);
      }

    }
    configResponse.setFailedToGetSharedConfig(true);
    return configResponse;
  }

  /**
   * Create a response containing the status of the Shared configuration and information about other
   * locators containing newer shared configuration data (if at all)
   * @return {@link SharedConfigurationStatusResponse} containing the {@link
   * SharedConfigurationStatus}
   */
  public SharedConfigurationStatusResponse createStatusResponse() {
    SharedConfigurationStatusResponse response = new SharedConfigurationStatusResponse();
    response.setStatus(getStatus());
    response.addWaitingLocatorInfo(newerSharedConfigurationLocatorInfo);
    return response;
  }

  /**
   * Deletes the xml entity from the shared configuration.
   */
  public void deleteXmlEntity(final XmlEntity xmlEntity, String[] groups) throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    // No group is specified, so delete in every single group if it exists.
    if (groups == null) {
      Set<String> groupSet = configRegion.keySet();
      groups = groupSet.toArray(new String[groupSet.size()]);
    }
    for (String group : groups) {
      Configuration configuration = (Configuration) configRegion.get(group);
      if (configuration != null) {
        String xmlContent = configuration.getCacheXmlContent();
        if (xmlContent != null && !xmlContent.isEmpty()) {
          Document doc = createAndUpgradeDocumentFromXml(xmlContent);
          XmlUtils.deleteNode(doc, xmlEntity);
          configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
          configRegion.put(group, configuration);
          //writeConfig(configuration);
        }
      }
    }
  }

  public void modifyCacheAttributes(final XmlEntity xmlEntity, String[] groups) throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    // No group is specified, so modify the cache attributes for a in every single group if it
    // exists.
    if (groups == null) {
      Set<String> groupSet = configRegion.keySet();
      groups = groupSet.toArray(new String[groupSet.size()]);
    }
    for (String group : groups) {
      Configuration configuration = (Configuration) configRegion.get(group);

      if (configuration == null) {
        configuration = new Configuration(group);
      }
      String xmlContent = configuration.getCacheXmlContent();
      if (xmlContent == null || xmlContent.isEmpty()) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        CacheXmlGenerator.generateDefault(pw);
        xmlContent = sw.toString();
      }

      Document doc = createAndUpgradeDocumentFromXml(xmlContent);

      // Modify the cache attributes
      XmlUtils.modifyRootAttributes(doc, xmlEntity);

      // Change the xml content of the configuration and put it the config region
      configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
      configRegion.put(group, configuration);
      //writeConfig(configuration);
    }
  }

  /**
   * For tests only. TODO: clean this up and remove from production code
   * <p/>
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
        File file = new File(configDiskDirPath);
        FileUtils.deleteDirectory(file);
      }
      FileUtils.deleteDirectory(new File(configDirPath));
    } catch (Exception exception) {
      throw new AssertionError(exception);
    }
  }

  public byte[] getJarBytesFromThisLocator(String group, String jarName) throws Exception {
    Configuration configuration = getConfiguration(group);

    //TODO: Should we check  like this, or just check jar.exists below()?
    if (configuration == null || !configuration.getJarNames().contains(jarName)) {
      return null;
    }

    File jar = getPathToJarOnThisLocator(group, jarName).toFile();
    return FileUtils.readFileToByteArray(jar);
  }

  public Path getPathToJarOnThisLocator(String groupName, String jarName) {
    return new File(configDirPath).toPath().resolve(groupName).resolve(jarName);
  }

  public Map<String, byte[]> getAllJarsFromThisLocator(Set<String> groups) throws Exception {
    Map<String, byte[]> jarNamesToJarBytes = new HashMap<>();

    for (String group : groups) {
      Configuration groupConfig = getConfiguration(group);
      if (groupConfig == null) {
        break;
      }

      Set<String> jars = groupConfig.getJarNames();
      for (String jar : jars) {
        byte[] jarBytes = getJarBytesFromThisLocator(group, jar);
        jarNamesToJarBytes.put(jar, jarBytes);
      }
    }

    return jarNamesToJarBytes;
  }

  public Configuration getConfiguration(String groupName) {
    Configuration configuration = getConfigurationRegion().get(groupName);
    return configuration;
  }

  public Map<String, Configuration> getEntireConfiguration() throws Exception {
    Set<String> keys = getConfigurationRegion().keySet();
    return getConfigurationRegion().getAll(keys);
  }

  /**
   * Returns the path of Shared configuration directory
   * @return {@link String} path of the shared configuration directory
   */
  public String getSharedConfigurationDirPath() {
    return configDirPath;
  }

  /**
   * Gets the current status of the SharedConfiguration If the status is started , it determines if
   * the shared configuration is waiting for new configuration on other locators
   * @return {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatus getStatus() {
    SharedConfigurationStatus scStatus = this.status.get();
    if (scStatus == SharedConfigurationStatus.STARTED) {
      PersistentMemberManager pmm = cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = pmm.getWaitingRegions();
      if (!waitingRegions.isEmpty()) {
        this.status.compareAndSet(SharedConfigurationStatus.STARTED,
            SharedConfigurationStatus.WAITING);
        Set<PersistentMemberID> persMemIds =
            waitingRegions.get(Region.SEPARATOR_CHAR + CONFIG_REGION_NAME);
        for (PersistentMemberID persMemId : persMemIds) {
          newerSharedConfigurationLocatorInfo.add(new PersistentMemberPattern(persMemId));
        }
      }
    }
    return this.status.get();
  }

  /**
   * Loads the
   */
  public void loadSharedConfigurationFromDisk() throws Exception {
    Map<String, Configuration> sharedConfigurationMap = readSharedConfigurationFromDisk();
    getConfigurationRegion().clear();
    getConfigurationRegion().putAll(sharedConfigurationMap);
  }

  public void modifyProperties(final Properties properties, String[] groups) throws Exception {
    if (groups == null) {
      groups = new String[]{SharedConfiguration.CLUSTER_CONFIG};
    }
    Region<String, Configuration> configRegion = getConfigurationRegion();
    for (String group : groups) {
      Configuration configuration = configRegion.get(group);
      if (configuration == null) {
        configuration = new Configuration(group);
      }
      configuration.getGemfireProperties().putAll(properties);
      configRegion.put(group, configuration);
      //writeConfig(configuration);
    }
  }

  /**
   * Removes the jar files from the shared configuration.
   * @param jarNames Names of the jar files.
   * @param groups Names of the groups which had the jar file deployed.
   * @return true on success.
   */
  public boolean removeJars(final String[] jarNames, String[] groups) {
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
        configuration.removeJarNames(jarNames);
        configRegion.put(group, configuration);
      }
    } catch (Exception e) {
      logger.info("Exception occurred while deleting the jar files", e);
      success = false;
    }
    return success;
  }

  public void renameExistingSharedConfigDirectory() {
    File configDirFile = new File(configDirPath);
    if (configDirFile.exists()) {
      String configDirFileName2 = CLUSTER_CONFIG_ARTIFACTS_DIR_NAME
          + new SimpleDateFormat("yyyyMMddhhmm").format(new Date()) + "." + System.nanoTime();
      File configDirFile2 = new File(configDirFile.getParent(), configDirFileName2);
      try {
        FileUtils.moveDirectory(configDirFile, configDirFile2);
      } catch (IOException e) {
        logger.info(e);
      }
    }
  }

  /**
   * Creates a directory for this configuration if it doesn't already exist.
   */
  private File createConfigDirIfNecessary(final String configName) throws Exception {
    File clusterConfigDir = new File(getSharedConfigurationDirPath());
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

  public void writeConfig(final Configuration configuration) throws Exception {
    File configDir = createConfigDirIfNecessary(configuration.getConfigName());

    File propsFile = new File(configDir, configuration.getPropertiesFileName());
    BufferedWriter bw = new BufferedWriter(new FileWriter(propsFile));
    configuration.getGemfireProperties().store(bw, null);
    bw.close();

    File xmlFile = new File(configDir, configuration.getCacheXmlFileName());
    FileUtils.writeStringToFile(xmlFile, configuration.getCacheXmlContent(), "UTF-8");
  }

  private boolean lockSharedConfiguration() {
    return sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, -1, -1);
  }

  private void unlockSharedConfiguration() {
    sharedConfigLockingService.unlock(SHARED_CONFIG_LOCK_NAME);
  }

  public void addJarFromOtherLocators(String groupName, String jarName) throws Exception {
    logger.info("Getting Jar files from other locators");
    DM dm = cache.getDistributionManager();
    DistributedMember me = cache.getMyId();
    Set<DistributedMember> locators =
        new HashSet<>(dm.getAllHostedLocatorsWithSharedConfiguration().keySet());
    locators.remove(me);

    createConfigDirIfNecessary(groupName);

    byte[] jarBytes = locators.stream()
        .map((DistributedMember locator) -> downloadJarFromLocator(locator, groupName, jarName))
        .filter(Objects::nonNull)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No locators have a deployed jar named " + jarName + " in " + groupName));

    File jarToWrite = getPathToJarOnThisLocator(groupName, jarName).toFile();
    FileUtils.writeByteArrayToFile(jarToWrite, jarBytes);
  }

  private byte[] downloadJarFromLocator(DistributedMember locator, String groupName,
                                        String jarName) {
    ResultCollector<byte[], List<byte[]>> rc = (ResultCollector<byte[], List<byte[]>>) CliUtil
        .executeFunction(new UploadJarFunction(), new Object[]{groupName, jarName}, locator);

    List<byte[]> result = rc.getResult();

    // we should only get one byte[] back in the list
    return result.stream()
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  /**
   * Gets the region containing the shared configuration data. The region is created , if it does
   * not exist already. Note : this could block if this locator contains stale persistent
   * configuration data.
   * @return {@link Region} ConfigurationRegion
   */
  private Region<String, Configuration> getConfigurationRegion() {
    Region<String, Configuration> configRegion = cache.getRegion(CONFIG_REGION_NAME);

    try {
      if (configRegion == null) {
        File diskDir = new File(configDiskDirPath);

        if (!diskDir.exists()) {
          if (!diskDir.mkdirs()) {
            throw new IOException("Cannot create directory at " + configDiskDirPath);
          }
        }

        File[] diskDirs = {diskDir};
        cache.createDiskStoreFactory().setDiskDirs(diskDirs).setAutoCompact(true)
            .setMaxOplogSize(10).create(CLUSTER_CONFIG_DISK_STORE_NAME);

        AttributesFactory<String, Configuration> regionAttrsFactory =
            new AttributesFactory<String, Configuration>();
        regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionAttrsFactory.setCacheListener(new ConfigurationChangeListener(this));
        regionAttrsFactory.setDiskStoreName(CLUSTER_CONFIG_DISK_STORE_NAME);
        regionAttrsFactory.setScope(Scope.DISTRIBUTED_ACK);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        internalArgs.setMetaRegionWithTransactions(false);

        configRegion =
            cache.createVMRegion(CONFIG_REGION_NAME, regionAttrsFactory.create(), internalArgs);
      }

    } catch (CancelException e) {
      if (configRegion == null) {
        this.status.set(SharedConfigurationStatus.STOPPED);
      }
      throw e; // CONFIG: don't rethrow as Exception, keep it a subclass of CancelException

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
   * @return {@link Configuration}
   */
  private Configuration readConfiguration(final String configName, final String configDirectory)
      throws SAXException, ParserConfigurationException, TransformerFactoryConfigurationError,
      TransformerException {
    Configuration configuration = new Configuration(configName);
    String cacheXmlFullPath =
        FilenameUtils.concat(configDirectory, configuration.getCacheXmlFileName());
    String propertiesFullPath =
        FilenameUtils.concat(configDirectory, configuration.getPropertiesFileName());

    File file = new File(configDirectory);
    String[] jarFileNames = file.list(jarFileFilter);

    if (jarFileNames != null && jarFileNames.length != 0) {
      configuration.addJarNames(jarFileNames);
    }

    try {
      configuration.setCacheXmlContent(XmlUtils.readXmlAsStringFromFile(cacheXmlFullPath));
      configuration.setGemfireProperties(readProperties(propertiesFullPath));
    } catch (IOException e) {
      logger.info(e);
    }
    return configuration;
  }

  /**
   * Reads the properties from the properties file.
   * @return {@link Properties}
   */
  private Properties readProperties(final String propertiesFilePath) throws IOException {
    Properties properties = new Properties();
    File propsFile = new File(propertiesFilePath);
    FileInputStream fis = null;
    if (propsFile.exists()) {
      try {
        fis = new FileInputStream(propsFile);
        properties.load(fis);
      } finally {
        if (fis != null) {
          fis.close();
        }
      }
    }
    return properties;
  }

  /**
   * Reads the "shared_config" directory and loads all the cache.xml, gemfire.properties and
   * deployed jars information
   * @return {@link Map}
   */
  private Map<String, Configuration> readSharedConfigurationFromDisk() throws SAXException,
      ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
    String[] subdirectoryNames = getSubdirectories(configDirPath);
    Map<String, Configuration> sharedConfiguration = new HashMap<String, Configuration>();

    if (subdirectoryNames != null) {
      for (String subdirectoryName : subdirectoryNames) {
        String fullpath = FilenameUtils.concat(configDirPath, subdirectoryName);
        Configuration configuration = readConfiguration(subdirectoryName, fullpath);
        sharedConfiguration.put(subdirectoryName, configuration);
      }
    }
    return sharedConfiguration;
  }

  /**
   * Writes the
   * @param dirPath target directory , where the jar files are to be written
   * @param jarNames Array containing the name of the jar files.
   * @param jarBytes Array of byte arrays for the jar files.
   */
  private void writeJarFiles(final String dirPath, final String[] jarNames,
                             final byte[][] jarBytes) {
    for (int i = 0; i < jarNames.length; i++) {
      String filePath = FilenameUtils.concat(dirPath, jarNames[i]);
      File jarFile = new File(filePath);
      try {
        FileUtils.writeByteArrayToFile(jarFile, jarBytes[i]);
      } catch (IOException e) {
        logger.info(e);
      }
    }
  }

  /**
   * Create a {@link Document} using {@link XmlUtils#createDocumentFromXml(String)} and if the
   * version attribute is not equal to the current version then update the XML to the current schema
   * and return the document.
   * @param xmlContent XML content to load and upgrade.
   * @return {@link Document} from xmlContent.
   * @since GemFire 8.1
   */
  // UnitTest SharedConfigurationJUnitTest.testCreateAndUpgradeDocumentFromXml
  static Document createAndUpgradeDocumentFromXml(final String xmlContent)
      throws SAXException, ParserConfigurationException, IOException, XPathExpressionException {
    Document doc = XmlUtils.createDocumentFromXml(xmlContent);
    if (!CacheXml.VERSION_LATEST.equals(XmlUtils.getAttribute(doc.getDocumentElement(),
        CacheXml.VERSION, CacheXml.GEODE_NAMESPACE))) {
      doc = XmlUtils.upgradeSchema(doc, CacheXml.GEODE_NAMESPACE, CacheXml.LATEST_SCHEMA_LOCATION,
          CacheXml.VERSION_LATEST);
    }
    return doc;
  }

  /**
   * Returns an array containing the names of the subdirectories in a given directory
   * @param path Path of the directory whose subdirectories are listed
   * @return String[] names of first level subdirectories, null if no subdirectories are found or if
   * the path is incorrect
   */
  private static String[] getSubdirectories(String path) {
    File directory = new File(path);
    return directory.list(DirectoryFileFilter.INSTANCE);
  }

  private static class JarFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".jar");
    }
  }
}
