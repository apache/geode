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
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

@SuppressWarnings({"deprecation", "unchecked"})
public class ClusterConfigurationService {

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
  private final String configDiskDirPath;

  private final Set<PersistentMemberPattern> newerSharedConfigurationLocatorInfo = new HashSet<>();
  private final AtomicReference<SharedConfigurationStatus> status = new AtomicReference<>();

  private GemFireCacheImpl cache;
  private final DistributedLockService sharedConfigLockingService;

  public ClusterConfigurationService(Cache cache) throws IOException {
    this.cache = (GemFireCacheImpl) cache;
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

    // resolve the file paths
    String configDiskDirName =
        CLUSTER_CONFIG_DISK_DIR_PREFIX + cache.getDistributedSystem().getName();

    configDirPath = FilenameUtils.concat(clusterConfigRootDir, CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
    configDiskDirPath = FilenameUtils.concat(clusterConfigRootDir, configDiskDirName);
    sharedConfigLockingService = getSharedConfigLockService(cache.getDistributedSystem());
    status.set(SharedConfigurationStatus.NOT_STARTED);
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
    } catch (IllegalArgumentException e) {
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
        groups = new String[] {ClusterConfigurationService.CLUSTER_CONFIG};
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
        try {
          final Document doc = XmlUtils.createAndUpgradeDocumentFromXml(xmlContent);
          XmlUtils.addNewNode(doc, xmlEntity);
          configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
          configRegion.put(group, configuration);
        } catch (Exception e) {
          logger.error("error updating cluster configuration for group " + group, e);
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
        Configuration configuration = (Configuration) configRegion.get(group);
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
            logger.error("error updating cluster configuration for group " + group, e);
          }
        }
      }
    } finally {
      unlockSharedConfiguration();
    }
  }

  // we don't need to trigger the change listener for this modification, so it's ok to
  // operate on the original configuration object
  public void modifyXmlAndProperties(Properties properties, XmlEntity xmlEntity, String[] groups) {
    lockSharedConfiguration();
    try {
      if (groups == null) {
        groups = new String[] {ClusterConfigurationService.CLUSTER_CONFIG};
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
            logger.error("error updating cluster configuration for group " + group, e);
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
   * 
   * @return true on success
   */
  public boolean addJarsToThisLocator(String[] jarNames, byte[][] jarBytes, String[] groups) {
    boolean success = true;
    lockSharedConfiguration();
    try {
      if (groups == null) {
        groups = new String[] {ClusterConfigurationService.CLUSTER_CONFIG};
      }
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : groups) {
        Configuration configuration = configRegion.get(group);

        if (configuration == null) {
          configuration = new Configuration(group);
          createConfigDirIfNecessary(group);
        }

        String groupDir = FilenameUtils.concat(configDirPath, group);
        for (int i = 0; i < jarNames.length; i++) {
          String filePath = FilenameUtils.concat(groupDir, jarNames[i]);
          File jarFile = new File(filePath);
          try {
            FileUtils.writeByteArrayToFile(jarFile, jarBytes[i]);
          } catch (IOException e) {
            logger.info(e);
          }
        }

        // update the record after writing the jars to the file system, since the listener
        // will need the jars on file to upload to other locators. Need to update the jars
        // using a new copy of the Configuration so that the change listener will pick up the jar
        // name changes.
        Configuration configurationCopy = new Configuration(configuration);
        configurationCopy.addJarNames(jarNames);
        configRegion.put(group, configurationCopy);
      }
    } catch (Exception e) {
      success = false;
      logger.info(e.getMessage(), e);
    } finally {
      unlockSharedConfiguration();
    }
    return success;
  }

  /**
   * Removes the jar files from the shared configuration. used when undeploy jars
   *
   * @param jarNames Names of the jar files.
   * @param groups Names of the groups which had the jar file deployed.
   * @return true on success.
   */
  public boolean removeJars(final String[] jarNames, String[] groups) {
    boolean success = true;
    lockSharedConfiguration();
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

  /**
   * read the jar bytes in the file system
   */
  // used when creating cluster config response
  // and used when uploading the jars to another locator
  public byte[] getJarBytesFromThisLocator(String group, String jarName) throws Exception {
    Configuration configuration = getConfiguration(group);

    File jar = getPathToJarOnThisLocator(group, jarName).toFile();

    if (configuration == null || !configuration.getJarNames().contains(jarName) || !jar.exists()) {
      return null;
    }

    return FileUtils.readFileToByteArray(jar);
  }

  // used in the cluster config change listener when jarnames are changed in the internal region
  public void downloadJarFromOtherLocators(String groupName, String jarName) throws Exception {
    logger.info("Getting Jar files from other locators");
    DM dm = cache.getDistributionManager();
    DistributedMember me = cache.getMyId();
    Set<DistributedMember> locators =
        new HashSet<>(dm.getAllHostedLocatorsWithSharedConfiguration().keySet());
    locators.remove(me);

    createConfigDirIfNecessary(groupName);

    byte[] jarBytes = locators.stream()
        .map((DistributedMember locator) -> downloadJarFromLocator(locator, groupName, jarName))
        .filter(Objects::nonNull).findFirst().orElseThrow(() -> new IllegalStateException(
            "No locators have a deployed jar named " + jarName + " in " + groupName));

    File jarToWrite = getPathToJarOnThisLocator(groupName, jarName).toFile();
    FileUtils.writeByteArrayToFile(jarToWrite, jarBytes);
  }

  // used when creating cluster config response
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

  /**
   * Creates the shared configuration service
   * 
   * @param loadSharedConfigFromDir when set to true, loads the configuration from the share_config
   *        directory
   */
  public void initSharedConfiguration(boolean loadSharedConfigFromDir) throws Exception {
    status.set(SharedConfigurationStatus.STARTED);
    Region<String, Configuration> configRegion = this.getConfigurationRegion();
    lockSharedConfiguration();
    try {
      if (loadSharedConfigFromDir) {
        logger.info("Reading cluster configuration from '{}' directory",
            ClusterConfigurationService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
        loadSharedConfigurationFromDisk();
      } else {
        persistSecuritySettings(configRegion);
        // for those groups that have jar files, need to download the jars from other locators
        // if it doesn't exist yet
        Set<String> groups = configRegion.keySet();
        for (String group : groups) {
          Configuration config = configRegion.get(group);
          for (String jar : config.getJarNames()) {
            if (!(getPathToJarOnThisLocator(group, jar).toFile()).exists()) {
              downloadJarFromOtherLocators(group, jar);
            }
          }
        }
      }
    } finally {
      unlockSharedConfiguration();
    }

    status.set(SharedConfigurationStatus.RUNNING);
  }

  private void persistSecuritySettings(final Region<String, Configuration> configRegion) {
    Properties securityProps = cache.getDistributedSystem().getSecurityProperties();

    Configuration clusterPropertiesConfig =
        configRegion.get(ClusterConfigurationService.CLUSTER_CONFIG);
    if (clusterPropertiesConfig == null) {
      clusterPropertiesConfig = new Configuration(ClusterConfigurationService.CLUSTER_CONFIG);
      configRegion.put(ClusterConfigurationService.CLUSTER_CONFIG, clusterPropertiesConfig);
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
   * requested shared configuration This method locks the ClusterConfigurationService
   */
  public ConfigurationResponse createConfigurationReponse(final ConfigurationRequest configRequest)
      throws Exception {

    ConfigurationResponse configResponse = new ConfigurationResponse();

    for (int i = 0; i < configRequest.getNumAttempts(); i++) {
      boolean isLocked = sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, 5000, 5000);
      try {
        if (isLocked) {
          Set<String> groups = configRequest.getGroups();
          groups.add(ClusterConfigurationService.CLUSTER_CONFIG);
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
   * 
   * @return {@link SharedConfigurationStatusResponse} containing the
   *         {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatusResponse createStatusResponse() {
    SharedConfigurationStatusResponse response = new SharedConfigurationStatusResponse();
    response.setStatus(getStatus());
    response.addWaitingLocatorInfo(newerSharedConfigurationLocatorInfo);
    return response;
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

  public Path getPathToJarOnThisLocator(String groupName, String jarName) {
    return new File(configDirPath).toPath().resolve(groupName).resolve(jarName);
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
   * 
   * @return {@link String} path of the shared configuration directory
   */
  public String getSharedConfigurationDirPath() {
    return configDirPath;
  }

  /**
   * Gets the current status of the ClusterConfigurationService If the status is started , it
   * determines if the shared configuration is waiting for new configuration on other locators
   * 
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
   * Loads the internal region with the configuration in the configDirPath
   */
  public void loadSharedConfigurationFromDisk() throws Exception {
    lockSharedConfiguration();
    File[] groupNames =
        new File(configDirPath).listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
    Map<String, Configuration> sharedConfiguration = new HashMap<String, Configuration>();

    try {
      for (File groupName : groupNames) {
        Configuration configuration = readConfiguration(groupName);
        sharedConfiguration.put(groupName.getName(), configuration);
      }
      Region clusterRegion = getConfigurationRegion();
      clusterRegion.clear();
      clusterRegion.putAll(sharedConfiguration);

      // Overwrite the security settings using the locator's properties, ignoring whatever
      // in the import
      persistSecuritySettings(clusterRegion);

    } finally {
      unlockSharedConfiguration();
    }
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


  // Write the content of xml and properties into the file system for exporting purpose
  public void writeConfigToFile(final Configuration configuration) throws Exception {
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


  private byte[] downloadJarFromLocator(DistributedMember locator, String groupName,
      String jarName) {
    ResultCollector<byte[], List<byte[]>> rc = (ResultCollector<byte[], List<byte[]>>) CliUtil
        .executeFunction(new UploadJarFunction(), new Object[] {groupName, jarName}, locator);

    List<byte[]> result = rc.getResult();

    // we should only get one byte[] back in the list
    return result.stream().filter(Objects::nonNull).findFirst().orElse(null);
  }

  /**
   * Gets the region containing the shared configuration data. The region is created , if it does
   * not exist already. Note : this could block if this locator contains stale persistent
   * configuration data.
   * 
   * @return {@link Region} ConfigurationRegion, this should never be null
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

  // check if it's ok from populate the properties from one member to another
  public static boolean isMisConfigured(Properties fromProps, Properties toProps, String key) {
    String fromPropValue = fromProps.getProperty(key);
    String toPropValue = toProps.getProperty(key);

    // if this to prop is not specified, this is always OK.
    if (org.apache.commons.lang.StringUtils.isBlank(toPropValue))
      return false;

    // to props is not blank, but from props is blank, NOT OK.
    if (org.apache.commons.lang.StringUtils.isBlank(fromPropValue))
      return true;

    // at this point check for eqality
    return !fromPropValue.equals(toPropValue);
  }
}
