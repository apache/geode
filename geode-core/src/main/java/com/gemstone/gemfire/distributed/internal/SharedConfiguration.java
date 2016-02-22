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
package com.gemstone.gemfire.distributed.internal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberManager;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.functions.ImportSharedConfigurationArtifactsFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.callbacks.ConfigurationChangeListener;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.configuration.functions.GetAllJarsFunction;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationResponse;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils;
import com.gemstone.gemfire.management.internal.configuration.utils.ZipUtils;

/*********
 * 
 *
 */
@SuppressWarnings("deprecation")
public class SharedConfiguration {

  private static final Logger logger = LogService.getLogger();
  
  static class JarFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".jar");
    }
  }
  /****
   * Name of the directory where the shared configuration artifacts are stored
   */
  public static final String CLUSTER_CONFIG_ARTIFACTS_DIR_NAME = "cluster_config";
  public static final String CLUSTER_CONFIG_DISK_STORE_NAME = "cluster_config";
  public static String CONFIG_DIR_PATH;//FilenameUtils.concat(System.getProperty("user.dir"), CONFIG_ARTIFACTS_DIR_NAME);
  public static final String CLUSTER_CONFIG_DISK_DIR_PREFIX = "ConfigDiskDir_";

  public static final String CLUSTER_CONFIG = "cluster";


  /***
   * Name of the lock service used for shared configuration
   */
  public static final String SHARED_CONFIG_LOCK_SERVICE_NAME = "__CLUSTER_CONFIG_LS";

  /***
   * Name of the lock for locking the shared configuration
   */
  public static final String SHARED_CONFIG_LOCK_NAME = "__CLUSTER_CONFIG_LOCK";

  /***
   * Name of the region which is used to store the configuration information
   */
  public static final String CONFIG_REGION_NAME = "_ConfigurationRegion";


  public String CONFIG_DISK_DIR_NAME;
  public String CONFIG_DISK_DIR_PATH;;

  private final Set<PersistentMemberPattern> newerSharedConfigurationLocatorInfo = new HashSet<PersistentMemberPattern>();
  private final AtomicReference<SharedConfigurationStatus> status = new AtomicReference<SharedConfigurationStatus>();
  private static final GetAllJarsFunction getAllJarsFunction = new GetAllJarsFunction();
  private static final JarFileFilter jarFileFilter = new JarFileFilter();

  private  GemFireCacheImpl cache;
  private final DistributedLockService sharedConfigLockingService;


  /****
   * Gets or creates (if not created) shared configuration lock service 
   * @return DistributedLockService 
   */

  public static DistributedLockService getSharedConfigLockService(DistributedSystem ds) {
    DistributedLockService sharedConfigDls = DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    try {
      if (sharedConfigDls == null) {
        sharedConfigDls = DLockService.create(SHARED_CONFIG_LOCK_SERVICE_NAME, (InternalDistributedSystem) ds, true, true);
      }
    } catch (IllegalArgumentException e) {
      return  DLockService.getServiceNamed(SHARED_CONFIG_LOCK_SERVICE_NAME);
    }
    return sharedConfigDls;
  }

  /**
   * Returns an array containing the names of the subdirectories in a given directory
   * @param path Path of the directory whose subdirectories are listed
   * @return String[] names of first level subdirectories, null if no subdirectories are found or if the path is incorrect
   */
  private static String[] getSubdirectories(String path) {
    File directory = new File(path);
    return directory.list(DirectoryFileFilter.INSTANCE);
  }

  public SharedConfiguration(Cache cache) throws IOException {
    this.cache = (GemFireCacheImpl)cache;
    CONFIG_DISK_DIR_NAME = CLUSTER_CONFIG_DISK_DIR_PREFIX + cache.getDistributedSystem().getName();
    String clusterConfigDir = cache.getDistributedSystem().getProperties().getProperty(DistributionConfig.CLUSTER_CONFIGURATION_DIR);
    if (StringUtils.isBlank(clusterConfigDir)) {
      clusterConfigDir = System.getProperty("user.dir");
    } else {
      File diskDir = new File(clusterConfigDir);
      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory : " + clusterConfigDir);
      }
      clusterConfigDir = diskDir.getCanonicalPath();
    }
    CONFIG_DISK_DIR_PATH = FilenameUtils.concat(clusterConfigDir, CONFIG_DISK_DIR_NAME);
    CONFIG_DIR_PATH = FilenameUtils.concat(clusterConfigDir, CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
    sharedConfigLockingService = getSharedConfigLockService(cache.getDistributedSystem());
    status.set(SharedConfigurationStatus.NOT_STARTED);
  }

  /*****
   * Add jar information into the shared configuration and save the jars in the file system
   * @param jarNames
   * @param jarBytes
   * @param groups
   * @return true on success
   */
  public boolean addJars(String []jarNames, byte[][]jarBytes, String[]groups)  {
    boolean success = true;
    try {
      if (groups == null) {
        groups = new String[] {SharedConfiguration.CLUSTER_CONFIG};
      }
      Region<String, Configuration> configRegion = getConfigurationRegion();
      for (String group : groups) {
        Configuration configuration = (Configuration) configRegion.get(group);

        if (configuration == null) {
          configuration = new Configuration(group);
          writeConfig(configuration);
        }
        configuration.addJarNames(jarNames);
        configRegion.put(group, configuration);
        String groupDir = FilenameUtils.concat(CONFIG_DIR_PATH, group);
        writeJarFiles(groupDir, jarNames, jarBytes);
      }
    } catch (Exception e) {
      success = false;
      logger.info(e.getMessage(), e);
    }
    return success;
  }


  /***
   * Adds/replaces the xml entity in the shared configuration
   * @param xmlEntity
   * @param groups
   * @throws Exception
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
      writeConfig(configuration);
    }
  }

  /**
   * Create a {@link Document} using
   * {@link XmlUtils#createDocumentFromXml(String)} and if the version attribute
   * is not equal to the current version then update the XML to the current
   * schema and return the document.
   * 
   * @param xmlContent
   *          XML content to load and upgrade.
   * @return {@link Document} from xmlContent.
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   * @throws XPathExpressionException 
   * @since 8.1
   */
  // UnitTest SharedConfigurationJUnitTest.testCreateAndUpgradeDocumentFromXml
  static Document createAndUpgradeDocumentFromXml(final String xmlContent) throws SAXException, ParserConfigurationException, IOException, XPathExpressionException {
    Document doc = XmlUtils.createDocumentFromXml(xmlContent);
    if (!CacheXml.VERSION_LATEST.equals(XmlUtils.getAttribute(doc.getDocumentElement(), CacheXml.VERSION, CacheXml.NAMESPACE))) {
      doc = XmlUtils.upgradeSchema(doc, CacheXml.NAMESPACE, CacheXml.LATEST_SCHEMA_LOCATION, CacheXml.VERSION_LATEST);
    }
    return doc;
  }

  public void clearSharedConfiguration() throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    if (configRegion != null) {
      configRegion.clear();
    }
  }


  /*****
   * Creates the shared configuration service
   * @param loadSharedConfigFromDir when set to true, loads the configuration from the share_config directory
   * @throws Exception 
   */
  public void initSharedConfiguration(boolean loadSharedConfigFromDir) throws Exception {
    status.set(SharedConfigurationStatus.STARTED);
    Region<String, Configuration> configRegion = this.getConfigurationRegion();

    if (loadSharedConfigFromDir) {
      lockSharedConfiguration();
      try {
        logger.info("Reading cluster configuration from '{}' directory", SharedConfiguration.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);

        Map<String, Configuration> sharedConfigMap = this.readSharedConfigurationFromDisk();
        final DM dm =  cache.getDistributedSystem().getDistributionManager();

        if (dm.getNormalDistributionManagerIds().isEmpty()) {
          Set<DistributedMember> locatorsWithSC = new HashSet<DistributedMember>(dm.getAllHostedLocatorsWithSharedConfiguration().keySet());

          //Send the config to other locators which host the shared configuration.
          if (!locatorsWithSC.isEmpty()) {
            final ImportSharedConfigurationArtifactsFunction fn = new ImportSharedConfigurationArtifactsFunction();
            final Date date = new Date();
            String zipFileName =  CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__FILE__NAME, new Timestamp(date.getTime()).toString());
            try {
              ZipUtils.zip(getSharedConfigurationDirPath(), zipFileName);
              File zipFile = new File(zipFileName);
              byte[] zipBytes = FileUtils.readFileToByteArray(zipFile);
              Object [] args = new Object[] {zipFileName, zipBytes};
              CliUtil.executeFunction(fn, args, locatorsWithSC);
            } catch (Exception e) {
              logger.error(e.getMessage(), e);
            }
          }
          //Clear the configuration region and load the configuration read from the 'shared_config' directory
          configRegion.clear();
          configRegion.putAll(sharedConfigMap);
        } 
      }finally {
        unlockSharedConfiguration();
      }
    } else {
      //Write out the existing configuration into the 'shared_config' directory
      //And get deployed jars from other locators.
      lockSharedConfiguration(); 
      try {
        Set<Entry<String, Configuration>> configEntries = configRegion.entrySet();

        for (Entry<String, Configuration> configEntry : configEntries) {
          Configuration configuration = configEntry.getValue();
          try {
            this.writeConfig(configuration);
          } catch (Exception e) {
            logger.info(e.getMessage(), e);
          }
        }
        logger.info("Completed writing the shared configuration to 'cluster_config' directory");
        this.getAllJarsFromOtherLocators();
      } finally {
        unlockSharedConfiguration();
      }
    }

    status.set(SharedConfigurationStatus.RUNNING);
  }


  public boolean lockSharedConfiguration() {
    return sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, -1, -1);
  }

  public void unlockSharedConfiguration() {
    sharedConfigLockingService.unlock(SHARED_CONFIG_LOCK_NAME);
  }

  /****
   * Creates a ConfigurationResponse based on the configRequest, configuration response contains the requested shared configuration
   * This method locks the SharedConfiguration
   * @param configRequest
   * @return ConfigurationResponse
   * @throws Exception 
   */
  public ConfigurationResponse createConfigurationReponse(ConfigurationRequest configRequest) throws Exception {

    ConfigurationResponse configResponse = new ConfigurationResponse();

    for (int i=0; i<configRequest.getNumAttempts(); i++) {
      boolean isLocked = sharedConfigLockingService.lock(SHARED_CONFIG_LOCK_NAME, 5000, 5000);
      try {
        if (isLocked) {
          logger.info("Building up configuration response with following configurations");
          Set<String> groups = configRequest.getGroups();
          groups.add(SharedConfiguration.CLUSTER_CONFIG);

          for (String group : groups) {
            Configuration configuration = getConfiguration(group);
            configResponse.addConfiguration(configuration);
          }

          Object[] jars = getAllJars(groups);
          if (jars != null) {
            String[] jarNames = (String[])jars[0];
            byte[][] jarBytes = (byte[][]) jars[1];
            configResponse.addJarsToBeDeployed(jarNames, jarBytes);
          }
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

  /***
   * Create a response containing the status of the Shared configuration and information about other locators containing newer
   * shared configuration data (if at all)
   * @return {@link SharedConfigurationStatusResponse} containing the {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatusResponse createStatusResponse() {
    SharedConfigurationStatusResponse response = new SharedConfigurationStatusResponse();
    response.setStatus(getStatus());
    response.addWaitingLocatorInfo(newerSharedConfigurationLocatorInfo);
    return response;
  }

  /*****
   * Deletes the xml entity from the shared configuration.
   * @param xmlEntity 
   * @param groups
   * @throws Exception
   */
  public void deleteXmlEntity (XmlEntity xmlEntity, String[] groups) throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    //No group is specified, so delete in every single group if it exists.
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
          writeConfig(configuration);
        }
      } 
    }
  }

  public void modifyCacheAttributes(XmlEntity xmlEntity, String [] groups) throws Exception {
    Region<String, Configuration> configRegion = getConfigurationRegion();
    //No group is specified, so modify the cache attributes for a in every single group if it exists.
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

      //Modify the cache attributes
      XmlUtils.modifyRootAttributes(doc, xmlEntity);

      //Change the xml content of the configuration and put it the config region
      configuration.setCacheXmlContent(XmlUtils.prettyXml(doc));
      configRegion.put(group, configuration);
      writeConfig(configuration);
    }
  }

  /***
   * Only to be used for clean up in DUnits. 
   */
  public void destroySharedConfiguration() {
    Region<String, Configuration> configRegion;
    try {
      configRegion = getConfigurationRegion();
      if (configRegion != null) {
        configRegion.destroyRegion();
      }
      DiskStore configDiskStore = this.cache.findDiskStore(CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
      if (configDiskStore != null) {
        configDiskStore.destroy();
        File file = new File(CONFIG_DISK_DIR_PATH);
        FileUtils.deleteDirectory(file);
      }
      FileUtils.deleteDirectory(new File(CONFIG_DIR_PATH));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }

  public Object[] getAllJars(Set<String> groups) throws Exception {
    Set<String> jarsAdded = new HashSet<String>();
    Object[] jars = new Object[2];

    for (String group : groups) {
      Configuration configuration = getConfiguration(group);
      if (configuration != null) {
        jarsAdded.addAll(configuration.getJarNames());
      }
    }
    int numJars = jarsAdded.size();
    jarsAdded.clear();

    if (numJars > 0) {
      String [] jarNames = new String[numJars];
      byte[][] jarBytes = new byte[numJars][];
      int ctr = 0;

      for (String group : groups) {
        Configuration configuration = getConfiguration(group);
        if (configuration != null) {
          Set<String> jarNameSet = configuration.getJarNames();
          for (String jarName : jarNameSet) {
            String groupDirPath = FilenameUtils.concat(CONFIG_DIR_PATH, group);
            if (!jarsAdded.contains(jarName)) {
              String jarFilePath = FilenameUtils.concat(groupDirPath, jarName);
              jarNames[ctr]=jarName;
              jarBytes[ctr] = FileUtils.readFileToByteArray(new File(jarFilePath));
              ctr++;
            }
          }
        }
      }
      jars[0] = jarNames;
      jars[1] = jarBytes;
    }
    return jars;
  }


  /***
   * Gets the Jar from existing locators in the system
   * @throws Exception 
   */
  public void getAllJarsFromOtherLocators() throws Exception {
    logger.info("Getting Jar files from other locators");
    DM dm = cache.getDistributionManager();
    DistributedMember me = cache.getMyId();
    Set<DistributedMember> locators = new HashSet<DistributedMember>(dm.getAllHostedLocatorsWithSharedConfiguration().keySet());
    locators.remove(me);
    String [] jarNames = null;
    byte [][] jarBytes = null;

    if (locators.isEmpty()) {
      logger.info("No other locators present");
      return;
    }
    @SuppressWarnings("unchecked")
    ResultCollector<?, List<Object>> rc = (ResultCollector<?, List<Object>>) CliUtil.executeFunction(getAllJarsFunction, null , locators);

    List<Object> results = rc.getResult();
    for (Object result : results) {
      if (result != null) {
        if (!(result instanceof Exception)) {
          Object[] jars = (Object[]) result;
          jarNames = (String[])jars[0];
          jarBytes = (byte[][]) jars[1];
          break;
        } 
      }
    }

    if (jarNames != null && jarBytes != null) {
      Map<String, Integer> jarIndex = new HashMap<String, Integer>();

      for (int i=0; i<jarNames.length; i++) {
        String jarName = jarNames[i];
        jarIndex.put(jarName, i);
      }

      Map<String, Configuration> entireConfiguration = getEntireConfiguration();
      Set<String> groups = entireConfiguration.keySet();

      for (String group : groups) {
        Configuration config = entireConfiguration.get(group);
        Set<String> groupJarNames = config.getJarNames();
        String groupDirPath = FilenameUtils.concat(CONFIG_DIR_PATH, group);

        for (String groupJarName : groupJarNames) {
          Integer index = jarIndex.get(groupJarName);

          if (index != null) {
            String jarFilePath = FilenameUtils.concat(groupDirPath, groupJarName);
            byte[] jarData = jarBytes[index.intValue()];

            try {
              FileUtils.writeByteArrayToFile(new File(jarFilePath), jarData);
            } catch (IOException e) {
              logger.info(e.getMessage(), e);
            }
          } else {
            //This should NEVER happen
            logger.error("JarFile {} not delivered.", groupJarName);
          }
        }
      }
    } else {
      logger.info("No deployed jars found on other locators.");
    }
  }

  public Configuration getConfiguration(String groupName) throws Exception {
    Configuration configuration = (Configuration)getConfigurationRegion().get(groupName);
    return configuration;
  }

  /*****
   * Gets the region containing the shared configuration data.
   * The region is created , if it does not exist already.
   * Note : this could block if this locator contains stale persistent configuration data.
   * @return  {@link Region} ConfigurationRegion
   * @throws Exception 
   */
  private Region<String, Configuration> getConfigurationRegion() throws Exception {
    @SuppressWarnings("unchecked")
    Region<String, Configuration> configRegion = cache.getRegion(CONFIG_REGION_NAME);

    try {
      if (configRegion == null) {
        File diskDir = new File(CONFIG_DISK_DIR_PATH);

        if (!diskDir.exists()) {
          if (!diskDir.mkdirs()) {
            throw new IOException("Cannot create directory at " + CONFIG_DISK_DIR_PATH);
          }
        }

        File [] diskDirs = {diskDir}; 
        cache.createDiskStoreFactory()
        .setDiskDirs(diskDirs)
        .setAutoCompact(true)
        .setMaxOplogSize(10)
        .create(CLUSTER_CONFIG_DISK_STORE_NAME);

        AttributesFactory<String, Configuration> regionAttrsFactory = new AttributesFactory<String, Configuration>();
        regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        regionAttrsFactory.setCacheListener(new ConfigurationChangeListener(this));
        regionAttrsFactory.setDiskStoreName(CLUSTER_CONFIG_DISK_STORE_NAME);
        regionAttrsFactory.setScope(Scope.DISTRIBUTED_ACK);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        internalArgs.setMetaRegionWithTransactions(false);

        configRegion = cache.createVMRegion(CONFIG_REGION_NAME, regionAttrsFactory.create(), internalArgs);
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
      throw new Exception("Error occurred while initializing cluster configuration", e);
    }
    return configRegion;
  }

  public Map<String, Configuration> getEntireConfiguration() throws Exception {
    Set<String> keys = getConfigurationRegion().keySet();
    return getConfigurationRegion().getAll(keys);
  }


  /****
   * Returns the path of Shared configuration directory
   * @return {@link String}  path of the shared configuration directory
   */
  public String getSharedConfigurationDirPath() {
    return CONFIG_DIR_PATH;
  }


  /*****
   * Gets the current status of the SharedConfiguration
   * If the status is started , it determines if the shared configuration is waiting for new configuration on 
   * other locators
   * @return {@link SharedConfigurationStatus}
   */
  public SharedConfigurationStatus getStatus() {
    SharedConfigurationStatus scStatus = this.status.get();
    if (scStatus == SharedConfigurationStatus.STARTED) {
      PersistentMemberManager pmm = cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = pmm.getWaitingRegions();
      if (!waitingRegions.isEmpty()) {
        this.status.compareAndSet(SharedConfigurationStatus.STARTED, SharedConfigurationStatus.WAITING);
        Set<PersistentMemberID> persMemIds =  waitingRegions.get(Region.SEPARATOR_CHAR + CONFIG_REGION_NAME);
        for (PersistentMemberID persMemId : persMemIds) {
          newerSharedConfigurationLocatorInfo.add(new PersistentMemberPattern(persMemId));
        }
      }
    }
    return this.status.get();
  }
  /****
   * Loads the 
   * @throws Exception 
   */
  public void loadSharedConfigurationFromDisk() throws Exception {
    Map<String, Configuration> sharedConfigurationMap = readSharedConfigurationFromDisk();
    getConfigurationRegion().clear();
    getConfigurationRegion().putAll(sharedConfigurationMap);
  }

  public void modifyProperties(Properties properties, String[] groups) throws Exception {
    if (groups == null) {
      groups = new String[] {SharedConfiguration.CLUSTER_CONFIG};
    }
    Region<String, Configuration> configRegion = getConfigurationRegion();
    for (String group : groups) {
      Configuration configuration = (Configuration) configRegion.get(group);
      if (configuration == null) {
        configuration = new Configuration(group);
      } 
      configuration.getGemfireProperties().putAll(properties);
      configRegion.put(group, configuration);
      writeConfig(configuration);
    }
  }

  /*****
   * Reads the configuration information from the shared configuration directory and returns a {@link Configuration} object
   * @param configName
   * @param configDirectory
   * @return {@link Configuration}
   * @throws TransformerException 
   * @throws TransformerFactoryConfigurationError 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  private Configuration readConfiguration(String configName, String configDirectory) throws SAXException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
    Configuration configuration = new Configuration(configName);
    String cacheXmlFullPath = FilenameUtils.concat(configDirectory, configuration.getCacheXmlFileName());
    String propertiesFullPath = FilenameUtils.concat(configDirectory, configuration.getPropertiesFileName());

    File file = new File(configDirectory);
    String [] jarFileNames = file.list(jarFileFilter);

    if (jarFileNames != null && jarFileNames.length != 0 ) {
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

  /*****
   * Reads the properties from the properties file.
   * @param propertiesFilePath
   * @return {@link Properties}
   * @throws IOException
   */
  public Properties readProperties(String propertiesFilePath) throws IOException{

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


  /****
   * Reads the "shared_config" directory and loads all the cache.xml , gemfire.properties and deployd jars information
   * @return {@link Map} 
   * @throws TransformerException 
   * @throws TransformerFactoryConfigurationError 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  private Map<String, Configuration> readSharedConfigurationFromDisk() throws SAXException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
    String []subdirectoryNames = getSubdirectories(CONFIG_DIR_PATH);
    Map<String, Configuration> sharedConfiguration = new HashMap<String, Configuration>();

    if (subdirectoryNames != null) {
      for (String subdirectoryName : subdirectoryNames) {
        String fullpath = FilenameUtils.concat(CONFIG_DIR_PATH, subdirectoryName);
        Configuration configuration = readConfiguration(subdirectoryName, fullpath);
        sharedConfiguration.put(subdirectoryName, configuration);
      }
    }
    return sharedConfiguration;
  }



  /****
   * Removes the jar files from the given directory     
   * @param dirPath Path of the configuration directory
   * @param jarNames Names of the jar files
   * @throws IOException
   */

  public void removeJarFiles (String dirPath, String[] jarNames) throws IOException {
    if (jarNames != null) {
      for (int i=0; i<jarNames.length; i++) {
        File jarFile = new File(FilenameUtils.concat(dirPath, jarNames[i]));
        if (jarFile.exists()) {
          FileUtils.forceDelete(jarFile);
        }
      }
    } else {
      File dir = new File(dirPath);
      String []jarFileNames = dir.list(jarFileFilter);
      if (jarFileNames.length != 0) {
        File jarFileToBeDeleted;
        for (String jarFileName : jarFileNames) {
          String fullPath = FilenameUtils.concat(dirPath, jarFileName);
          jarFileToBeDeleted = new File(fullPath);
          FileUtils.forceDelete(jarFileToBeDeleted);
        }
      }
    }
  }

  /****
   * Removes the jar files from the shared configuration. 
   * @param jarNames Names of the jar files.
   * @param groups Names of the groups which had the jar file deployed.
   * @return true on success.
   */
  public boolean removeJars(String []jarNames, String[] groups){
    boolean success = true;
    try {
      Region<String, Configuration> configRegion = getConfigurationRegion();
      if (groups == null) {
        Set<String> groupSet = configRegion.keySet();
        groups = groupSet.toArray(new String[groupSet.size()]);
      }
      for (String group : groups) {
        Configuration configuration = (Configuration) configRegion.get(group);
        if (configuration != null) {
          String dirPath = FilenameUtils.concat(getSharedConfigurationDirPath(), configuration.getConfigName());
          removeJarFiles(dirPath, jarNames);
        }
      }
      for (String group : groups) {
        Configuration configuration = (Configuration) configRegion.get(group);
        if (configuration != null) {
          if (!configuration.getJarNames().isEmpty()) {
            configuration.removeJarNames(jarNames);
            configRegion.put(group, configuration);
          }
        } 
      }
    } catch (Exception e) {
      logger.info("Exception occurred while deleting the jar files", e);
      success = false;
    }
    return success;
  }

  public void renameExistingSharedConfigDirectory() {
    File configDirFile = new File(CONFIG_DIR_PATH);
    if (configDirFile.exists()) {
      String configDirFileName2 = CLUSTER_CONFIG_ARTIFACTS_DIR_NAME + new SimpleDateFormat("yyyyMMddhhmm").format(new Date()) + "." + System.nanoTime(); 
      File configDirFile2 = new File(FilenameUtils.concat(configDirFileName2, configDirFileName2));
      try {
        FileUtils.moveDirectoryToDirectory(configDirFile, configDirFile2, true);
      } catch (IOException e) {
        logger.info(e);
      }
    } 
  }
  /***
   * Writes the cache.xml to the file , based on Configuration
   * @param dirPath Path of the directory in which the configuration is written
   * @param configuration 
   * @throws IOException 
   */
  private void writeCacheXml(String dirPath, Configuration configuration) throws IOException {
    String fullPath = FilenameUtils.concat(dirPath,configuration.getCacheXmlFileName());
    FileUtils.writeStringToFile(new File(fullPath), configuration.getCacheXmlContent(), "UTF-8") ;
  }
  /***
   * Writes the contents of the {@link Configuration} to the file system
   * @param configuration
   * @throws Exception
   */
  public void writeConfig(Configuration configuration) throws Exception {
    File configDir = new File(getSharedConfigurationDirPath());
    if (!configDir.exists()) {
      if (!configDir.mkdirs()) {
        throw new IOException("Cannot create directory : " + getSharedConfigurationDirPath());
      }
    }
    String dirPath = FilenameUtils.concat(getSharedConfigurationDirPath(), configuration.getConfigName());
    File file = new File(dirPath);
    if (!file.exists()) {
      if (!file.mkdir()) {
        throw new IOException("Cannot create directory : " + dirPath);
      }
    }

    writeProperties(dirPath, configuration);
    writeCacheXml(dirPath, configuration);
  }

  /*****
   * Writes the 
   * @param dirPath target directory , where the jar files are to be written
   * @param jarNames Array containing the name of the jar files.
   * @param jarBytes Array of byte arrays for the jar files.
   */
  private void writeJarFiles(String dirPath , String[] jarNames, byte[][] jarBytes) {
    for (int i=0; i<jarNames.length; i++) {
      String filePath = FilenameUtils.concat(dirPath, jarNames[i]);
      File jarFile = new File(filePath);
      try {
        FileUtils.writeByteArrayToFile(jarFile, jarBytes[i]);
      } catch (IOException e) {
        logger.info(e);
      }
    }
  }

  /****
   * Writes the properties to the file based on the {@link Configuration}
   * @param dirPath
   * @param configuration
   * @throws IOException
   */
  private void writeProperties(String dirPath, Configuration configuration) throws IOException {
    String fullPath = FilenameUtils.concat(dirPath,configuration.getPropertiesFileName());
    BufferedWriter bw = new BufferedWriter(new FileWriter(fullPath));
    configuration.getGemfireProperties().store(bw, "");
    bw.close();
  }

}
