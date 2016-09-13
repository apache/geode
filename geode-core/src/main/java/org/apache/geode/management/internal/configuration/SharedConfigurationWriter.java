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
package com.gemstone.gemfire.management.internal.configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.configuration.domain.ConfigurationChangeResult;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.configuration.functions.AddJarFunction;
import com.gemstone.gemfire.management.internal.configuration.functions.AddXmlEntityFunction;
import com.gemstone.gemfire.management.internal.configuration.functions.DeleteJarFunction;
import com.gemstone.gemfire.management.internal.configuration.functions.DeleteXmlEntityFunction;
import com.gemstone.gemfire.management.internal.configuration.functions.ModifyPropertiesFunction;

/***
 * Class for writing configuration changes to the Shared Configuration at the Locator(s).
 * This class is used in the Gfsh commands, to persist the configuration changes to the shared configuration hosted on locators.
 * 
 *
 */
public class SharedConfigurationWriter {
  private static final Logger logger = LogService.getLogger();
  
  private GemFireCacheImpl cache;
  private final AddJarFunction saveJarFunction = new AddJarFunction();
  private final DeleteJarFunction deleteJarFunction = new DeleteJarFunction();
  private final AddXmlEntityFunction addXmlEntityFunction = new AddXmlEntityFunction();
  private final DeleteXmlEntityFunction deleteXmlEntityFunction = new DeleteXmlEntityFunction();
  private final ModifyPropertiesFunction modifyPropertiesFunction = new ModifyPropertiesFunction();
  private boolean isSharedConfigEnabled;

  public SharedConfigurationWriter() {
    cache = GemFireCacheImpl.getInstance();
    isSharedConfigEnabled = cache.getDistributionManager().isSharedConfigurationServiceEnabledForDS();
  }
  
//  /***
//   * Adds or replaces the xml entity in the cache-xml for the specified groups in the shared configuration.
//   * @param xmlEntity XmlEntity that is to be added/replaced in the shared configuration
//   * @param groups target groups
//   * @return true on adding the xml-entity in the shared configuration
//   */
  public boolean addXmlEntity(XmlEntity xmlEntity, String[] groups) {
    Object[] args = new Object[2];
    args[0] = xmlEntity;
    args[1] = groups;
    return saveConfigChanges(addXmlEntityFunction, args);
  }
  
//  /*****
//   * Removes an xml entity from the cache-xml for the specified groups in the shared configuration.
//   * @param xmlEntity
//   * @param groups
//   * @return true on successful deletion
//   */
  public boolean deleteXmlEntity (XmlEntity xmlEntity, String[] groups) {
    Object[] args = new Object[2];
    args[0] = xmlEntity;
    args[1] = groups;
    return saveConfigChanges(deleteXmlEntityFunction, args);
  }
  
  
  public boolean modifyPropertiesAndCacheAttributes(Properties properties, XmlEntity xmlEntity, String[] groups) {
    Object[] args = new Object[3];
    args[0] = properties;
    args[1] = xmlEntity;
    args[2] = groups;
    return saveConfigChanges(modifyPropertiesFunction, args);
  }
  
//  /*****
//   * Adds the deployed jars to the shared configuration on all the locators
//   * @param jarNames Name of jar files to be added
//   * @param jarBytes Contents of jar files 
//   * @param groups   member groups on which these jars were deployed
//   * @return true when the jar files are saved on all the locators.
//   */
  public boolean addJars(String[] jarNames, byte[][]jarBytes, String[] groups) {
    Object [] args =  new Object[3];
    args[0] = jarNames;
    args[1] = jarBytes;
    args[2] = groups;
    return saveConfigChangesAllLocators(saveJarFunction, args);
  }

//  /****
//   * Deletes the jar files from the shared configuration on all the locators
//   * @param jarNames Name of the jar files to be deleted
//   * @param groups member groups on which these jars were undeployed
//   * @return true when the jar files are deleted from shared configuration on all the locators.
//   */
  public boolean deleteJars(String[] jarNames, String[] groups) {
    Object [] args =  new Object[3];
    args[0] = jarNames;
    args[1] = groups;
    return saveConfigChangesAllLocators(deleteJarFunction, args);
  }
  
  
  private boolean saveConfigChanges(Function function, Object[] args ) {
    if (!isSharedConfigEnabled) {
      return true;
    }
    boolean success = false;
    Set<DistributedMember> locators = new HashSet<DistributedMember>(cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());
   
    if (!locators.isEmpty()) {
      for (DistributedMember locator : locators) {
        ResultCollector<?, ?> rc = CliUtil.executeFunction(function, args, locator);
        @SuppressWarnings("unchecked")
        List<ConfigurationChangeResult> results = (List<ConfigurationChangeResult>) rc.getResult();
        if (!results.isEmpty()) {
          ConfigurationChangeResult configChangeResult = results.get(0);
          if (configChangeResult.isSuccessful()) {
            logger.info("Configuration change successful");
            success = true;
            break;
          } else {
            logger.info("Failed to save the configuration change. {}", configChangeResult);
            success = false;
          }
        }
      }
    }
    return success;
  }
  
  
  private boolean saveConfigChangesAllLocators(Function function, Object[] args) {
    if (!isSharedConfigEnabled) {
      return true;
    }
    boolean success = true;
    Set<DistributedMember> locators = new HashSet<DistributedMember>(cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());

    if (!locators.isEmpty()) {
      ResultCollector<?,?> rc = CliUtil.executeFunction(function, args, locators);
      @SuppressWarnings("unchecked")
      List<ConfigurationChangeResult> results = (List<ConfigurationChangeResult>) rc.getResult();

      if (!results.isEmpty()) {
        for (ConfigurationChangeResult configChangeResult : results) {
          if (!configChangeResult.isSuccessful()) {
            success = false;
            break;
          }
        }
      }
    }
    return success;
  }

}
