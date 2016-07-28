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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.FetchRegionAttributesFunction;
import com.gemstone.gemfire.management.internal.cli.functions.FetchRegionAttributesFunction.FetchRegionAttributesFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.RegionAlterFunction;
import com.gemstone.gemfire.management.internal.cli.functions.RegionCreateFunction;
import com.gemstone.gemfire.management.internal.cli.functions.RegionDestroyFunction;
import com.gemstone.gemfire.management.internal.cli.functions.RegionFunctionArgs;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.util.RegionPath;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 *
 * @since GemFire 7.0
 */
public class CreateAlterDestroyRegionCommands extends AbstractCommandsSupport {
  public static final Set<RegionShortcut> PERSISTENT_OVERFLOW_SHORTCUTS = new TreeSet<RegionShortcut>();
  
  static {
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_PERSISTENT_OVERFLOW);
  }

  @CliCommand (value = CliStrings.CREATE_REGION, help = CliStrings.CREATE_REGION__HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEODE_REGION, writesToSharedConfiguration = true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result createRegion(
      @CliOption (key = CliStrings.CREATE_REGION__REGION,
                  mandatory = true,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__REGION__HELP)
      String regionPath,
      @CliOption (key = CliStrings.CREATE_REGION__REGIONSHORTCUT,
                  mandatory = false,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__REGIONSHORTCUT__HELP)
      RegionShortcut regionShortcut,
      @CliOption (key = CliStrings.CREATE_REGION__USEATTRIBUTESFROM,
                  optionContext = ConverterHint.REGIONPATH,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__USEATTRIBUTESFROM__HELP)
      String useAttributesFrom, 
      @CliOption (key = CliStrings.CREATE_REGION__GROUP,
                  optionContext = ConverterHint.MEMBERGROUP,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] groups,
      @CliOption (key = CliStrings.CREATE_REGION__SKIPIFEXISTS,
                  unspecifiedDefaultValue = "true",
                  specifiedDefaultValue = "true",
                  help = CliStrings.CREATE_REGION__SKIPIFEXISTS__HELP)
      boolean skipIfExists,
      
      // the following should all be in alphabetical order according to
      // their key string
      @CliOption (key = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID,
                  help = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] asyncEventQueueIds,
      @CliOption (key = CliStrings.CREATE_REGION__CACHELISTENER,
                  help = CliStrings.CREATE_REGION__CACHELISTENER__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] cacheListener,
      @CliOption (key = CliStrings.CREATE_REGION__CACHELOADER,
                  help = CliStrings.CREATE_REGION__CACHELOADER__HELP)
      String cacheLoader,
      @CliOption (key = CliStrings.CREATE_REGION__CACHEWRITER,
                  help = CliStrings.CREATE_REGION__CACHEWRITER__HELP)
      String cacheWriter,
      @CliOption (key = CliStrings.CREATE_REGION__COLOCATEDWITH,
                  optionContext = ConverterHint.REGIONPATH,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__COLOCATEDWITH__HELP)
      String prColocatedWith,
      @CliOption (key = CliStrings.CREATE_REGION__COMPRESSOR,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__COMPRESSOR__HELP)
      String compressor,
      @CliOption (key = CliStrings.CREATE_REGION__CONCURRENCYLEVEL,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__CONCURRENCYLEVEL__HELP)
      Integer concurrencyLevel,
      @CliOption (key = CliStrings.CREATE_REGION__DISKSTORE,
                  help = CliStrings.CREATE_REGION__DISKSTORE__HELP)
      String diskStore,
      @CliOption (key = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION__HELP)
      Boolean enableAsyncConflation,
      @CliOption (key = CliStrings.CREATE_REGION__CLONINGENABLED,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__CLONINGENABLED__HELP)
      Boolean cloningEnabled,
      @CliOption (key = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED__HELP)
      Boolean concurrencyChecksEnabled,
      @CliOption (key = CliStrings.CREATE_REGION__MULTICASTENABLED,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__MULTICASTENABLED__HELP)
      Boolean mcastEnabled,
      @CliOption (key = CliStrings.CREATE_REGION__STATISTICSENABLED,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__STATISTICSENABLED__HELP)
      Boolean statisticsEnabled,
      @CliOption (key = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION__HELP)
      Boolean enableSubscriptionConflation,
      @CliOption (key = CliStrings.CREATE_REGION__DISKSYNCHRONOUS,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__DISKSYNCHRONOUS__HELP)
      Boolean diskSynchronous,
      @CliOption (key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME__HELP)
      Integer entryExpirationIdleTime,
      @CliOption (key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
                  help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP)
      String entryExpirationIdleTimeAction,
      @CliOption (key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP)
      Integer entryExpirationTTL,
      @CliOption (key = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION,
                  help = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION__HELP)
      String entryExpirationTTLAction,
      @CliOption (key = CliStrings.CREATE_REGION__GATEWAYSENDERID,
                  help = CliStrings.CREATE_REGION__GATEWAYSENDERID__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] gatewaySenderIds,
      @CliOption (key = CliStrings.CREATE_REGION__KEYCONSTRAINT,
                  help = CliStrings.CREATE_REGION__KEYCONSTRAINT__HELP)
      String keyConstraint,
      @CliOption (key = CliStrings.CREATE_REGION__LOCALMAXMEMORY,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__LOCALMAXMEMORY__HELP)
      Integer prLocalMaxMemory, 
      @CliOption (key = CliStrings.CREATE_REGION__OFF_HEAP,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "true",
                  help = CliStrings.CREATE_REGION__OFF_HEAP__HELP)
      Boolean offHeap,
      @CliOption (key = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME__HELP)
      Integer regionExpirationIdleTime,
      @CliOption (key = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION,
                  help = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP)
      String regionExpirationIdleTimeAction,
      @CliOption (key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL__HELP)
      Integer regionExpirationTTL,
      @CliOption (key = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION,
                  help = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION__HELP)
      String regionExpirationTTLAction,      
      @CliOption (key = CliStrings.CREATE_REGION__RECOVERYDELAY,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__RECOVERYDELAY__HELP)
      Long prRecoveryDelay,
      @CliOption (key = CliStrings.CREATE_REGION__REDUNDANTCOPIES,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__REDUNDANTCOPIES__HELP)
      Integer prRedundantCopies, 
      @CliOption (key = CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY__HELP)
      Long prStartupRecoveryDelay,
      @CliOption (key = CliStrings.CREATE_REGION__TOTALMAXMEMORY,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__TOTALMAXMEMORY__HELP)
      Long prTotalMaxMemory, 
      @CliOption (key = CliStrings.CREATE_REGION__TOTALNUMBUCKETS,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.CREATE_REGION__TOTALNUMBUCKETS__HELP)
      Integer prTotalNumBuckets,      
      @CliOption (key = CliStrings.CREATE_REGION__VALUECONSTRAINT,
                  help = CliStrings.CREATE_REGION__VALUECONSTRAINT__HELP)
      String valueConstraint
      // NOTICE: keep the region attributes params in alphabetical order
) {
    Result result = null;
    XmlEntity xmlEntity = null;

    try {
      Cache cache = CacheFactory.getAnyInstance();

      if (regionShortcut != null && useAttributesFrom != null) {
        throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__ONLY_ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_CAN_BE_SPECIFIED);
      } else if (regionShortcut == null && useAttributesFrom == null) {
        throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_IS_REQUIRED);
      }
      
      validateRegionPathAndParent(cache, regionPath);
      validateGroups(cache, groups);

      RegionFunctionArgs.ExpirationAttrs entryIdle = null;
      if (entryExpirationIdleTime != null) {
        entryIdle = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_IDLE, entryExpirationIdleTime, entryExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs entryTTL = null;
      if (entryExpirationTTL != null) {
        entryTTL = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_TTL, entryExpirationTTL, entryExpirationTTLAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionIdle = null;
      if (regionExpirationIdleTime != null) {
        regionIdle = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_IDLE, regionExpirationIdleTime, regionExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionTTL = null;
      if (regionExpirationTTL != null) {
        regionTTL = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_TTL, regionExpirationTTL, regionExpirationTTLAction);
      }
      
      RegionFunctionArgs regionFunctionArgs = null;
      if (useAttributesFrom != null) {
        if (!regionExists(cache, useAttributesFrom)) {
          throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND, new Object[] {CliStrings.CREATE_REGION__USEATTRIBUTESFROM, useAttributesFrom}));
        }
        
        
        FetchRegionAttributesFunctionResult<Object, Object> regionAttributesResult = getRegionAttributes(cache, useAttributesFrom);
        RegionAttributes<?, ?> regionAttributes = regionAttributesResult.getRegionAttributes();
           
        
        // give preference to user specified plugins than the ones retrieved from other region
        String[] cacheListenerClasses = cacheListener != null && cacheListener.length != 0 ? cacheListener : regionAttributesResult.getCacheListenerClasses();
        String cacheLoaderClass = cacheLoader != null ? cacheLoader : regionAttributesResult.getCacheLoaderClass();
        String cacheWriterClass = cacheWriter != null ? cacheWriter : regionAttributesResult.getCacheWriterClass();;

        regionFunctionArgs = new RegionFunctionArgs(regionPath,
            useAttributesFrom, skipIfExists, keyConstraint, valueConstraint,
            statisticsEnabled, entryIdle, entryTTL, regionIdle,
            regionTTL, diskStore, diskSynchronous, enableAsyncConflation,
            enableSubscriptionConflation, cacheListenerClasses, cacheLoaderClass,
            cacheWriterClass, asyncEventQueueIds, gatewaySenderIds,
            concurrencyChecksEnabled, cloningEnabled, concurrencyLevel, 
            prColocatedWith, prLocalMaxMemory, prRecoveryDelay,
            prRedundantCopies, prStartupRecoveryDelay,
            prTotalMaxMemory, prTotalNumBuckets,
            offHeap, mcastEnabled, regionAttributes);
        

        if (regionAttributes.getPartitionAttributes() == null && regionFunctionArgs.hasPartitionAttributes()) {
          throw new IllegalArgumentException(
              CliStrings.format(CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION, 
                                regionFunctionArgs.getPartitionArgs().getUserSpecifiedPartitionAttributes()) + " " +
              CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION, 
                                    useAttributesFrom));
        }
      } else {
        regionFunctionArgs = new RegionFunctionArgs(
          regionPath, regionShortcut, useAttributesFrom, skipIfExists,
          keyConstraint, valueConstraint, statisticsEnabled, entryIdle, entryTTL,
          regionIdle, regionTTL, diskStore, diskSynchronous,
          enableAsyncConflation, enableSubscriptionConflation, cacheListener,
          cacheLoader, cacheWriter, asyncEventQueueIds, gatewaySenderIds,
          concurrencyChecksEnabled, cloningEnabled, concurrencyLevel, 
          prColocatedWith, prLocalMaxMemory, prRecoveryDelay,
          prRedundantCopies, prStartupRecoveryDelay,
          prTotalMaxMemory, prTotalNumBuckets, null,compressor, offHeap , mcastEnabled);
        
        if (!regionShortcut.name().startsWith("PARTITION") && regionFunctionArgs.hasPartitionAttributes()) {
          throw new IllegalArgumentException(
              CliStrings.format(CliStrings.CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION, 
                                regionFunctionArgs.getPartitionArgs().getUserSpecifiedPartitionAttributes()) + " " +
              CliStrings.format(CliStrings.CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION, 
                                    useAttributesFrom));
        }
      }

      validateRegionFunctionArgs(cache, regionFunctionArgs);

      Set<DistributedMember> membersToCreateRegionOn = null;
      if (groups != null && groups.length != 0) {
        membersToCreateRegionOn = CliUtil.getDistributedMembersByGroup(cache, groups);
        // have only normal members from the group
        for (Iterator<DistributedMember> it = membersToCreateRegionOn.iterator(); it.hasNext();) {
          DistributedMember distributedMember = it.next();
          if ( ((InternalDistributedMember)distributedMember).getVmKind() == DistributionManager.LOCATOR_DM_TYPE ) {
            it.remove();
          }
        }
      } else {
        membersToCreateRegionOn = CliUtil.getAllNormalMembers(cache);
      }

      if (membersToCreateRegionOn.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(RegionCreateFunction.INSTANCE, regionFunctionArgs, membersToCreateRegionOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> regionCreateResults = (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult regionCreateResult : regionCreateResults) {
        boolean success  = regionCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", regionCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status", (success ? "" : errorPrefix) + regionCreateResult.getMessage());
        
        if (success) {
          xmlEntity = regionCreateResult.getXmlEntity();
        }
      }
      
      result = ResultBuilder.buildResult(tabularResultData);
      verifyDistributedRegionMbean(cache, regionPath); 
      
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
      
    }
    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, groups));
    }
    return result;
  }
  
  
  public boolean verifyDistributedRegionMbean(Cache cache, String regionName){
    GemFireCacheImpl gemfireCache = (GemFireCacheImpl) cache;
    int federationInterval = gemfireCache.getDistributedSystem().getConfig().getJmxManagerUpdateRate();    
    long timeEnd = System.currentTimeMillis() + federationInterval + 50;   

    for( ; System.currentTimeMillis() <= timeEnd ; ){
      try{
        DistributedRegionMXBean bean = ManagementService.getManagementService(cache).getDistributedRegionMXBean(regionName);
        if (bean == null){
          bean = ManagementService.getManagementService(cache).getDistributedRegionMXBean(Region.SEPARATOR + regionName);
        }        
        if(bean != null){
          return true;
        }else{
          Thread.sleep(2);         
        }
      }catch(Exception ex){
        continue;
      }
    }       
    return false;
  }  
  
  @CliCommand (value = CliStrings.ALTER_REGION, help = CliStrings.ALTER_REGION__HELP)
  @CliMetaData (relatedTopic = CliStrings.TOPIC_GEODE_REGION, writesToSharedConfiguration = true)
  public Result alterRegion(
      @CliOption (key = CliStrings.ALTER_REGION__REGION,
                  mandatory = true,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.ALTER_REGION__REGION__HELP)
      String regionPath,
      @CliOption (key = CliStrings.ALTER_REGION__GROUP,
                  optionContext = ConverterHint.MEMBERGROUP,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = CliStrings.ALTER_REGION__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] groups, 
      @CliOption (key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "-1",
                  help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME__HELP)
      Integer entryExpirationIdleTime,
      @CliOption (key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "INVALIDATE",
                  help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP)
      String entryExpirationIdleTimeAction,
      @CliOption (key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "-1",
                  help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP)
      Integer entryExpirationTTL,
      @CliOption (key = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "INVALIDATE",
                  help = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION__HELP)
      String entryExpirationTTLAction,
      @CliOption (key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "-1",
                  help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME__HELP)
      Integer regionExpirationIdleTime, 
      @CliOption (key = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "INVALIDATE",
                  help = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP)
      String regionExpirationIdleTimeAction,
      @CliOption (key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "-1",
                  help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL__HELP)
      Integer regionExpirationTTL, 
      @CliOption (key = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "INVALIDATE",
                  help = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION__HELP)
      String regionExpirationTTLAction,          
      @CliOption (key = CliStrings.ALTER_REGION__CACHELISTENER,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "",
                  help = CliStrings.ALTER_REGION__CACHELISTENER__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] cacheListeners,
      @CliOption (key = CliStrings.ALTER_REGION__CACHELOADER,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "null",
                  help = CliStrings.ALTER_REGION__CACHELOADER__HELP)
      String cacheLoader,
      @CliOption (key = CliStrings.ALTER_REGION__CACHEWRITER,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "null",
                  help = CliStrings.ALTER_REGION__CACHEWRITER__HELP)
      String cacheWriter,
      @CliOption (key = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "",
                  help = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] asyncEventQueueIds,
      @CliOption (key = CliStrings.ALTER_REGION__GATEWAYSENDERID,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "",
                  help = CliStrings.ALTER_REGION__GATEWAYSENDERID__HELP)
      @CliMetaData (valueSeparator = ",") 
      String[] gatewaySenderIds,
      @CliOption (key = CliStrings.ALTER_REGION__CLONINGENABLED,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "false",
                  help = CliStrings.ALTER_REGION__CLONINGENABLED__HELP)
      Boolean cloningEnabled,
      @CliOption (key = CliStrings.ALTER_REGION__EVICTIONMAX,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  specifiedDefaultValue = "0",
                  help = CliStrings.ALTER_REGION__EVICTIONMAX__HELP)
      Integer evictionMax) {
    Result result = null;
    XmlEntity xmlEntity = null;

    GeodeSecurityUtil.authorizeRegionManage(regionPath);

    try {
      Cache cache = CacheFactory.getAnyInstance();

      if (groups != null) {
        validateGroups(cache, groups);
      }

      RegionFunctionArgs.ExpirationAttrs entryIdle = null;
      if (entryExpirationIdleTime != null || entryExpirationIdleTimeAction != null) {
        if (entryExpirationIdleTime != null && entryExpirationIdleTime == -1) {
          entryExpirationIdleTime = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(entryExpirationIdleTimeAction)) {
          entryExpirationIdleTimeAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        entryIdle = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_IDLE,
            entryExpirationIdleTime, entryExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs entryTTL = null;
      if (entryExpirationTTL != null || entryExpirationTTLAction != null) {
        if (entryExpirationTTL != null && entryExpirationTTL == -1) {
          entryExpirationTTL = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(entryExpirationTTLAction)) {
          entryExpirationTTLAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        entryTTL = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.ENTRY_TTL,
            entryExpirationTTL, entryExpirationTTLAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionIdle = null;
      if (regionExpirationIdleTime != null || regionExpirationIdleTimeAction != null) {
        if (regionExpirationIdleTime != null  && regionExpirationIdleTime == -1) {
          regionExpirationIdleTime = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(regionExpirationIdleTimeAction)) {
          regionExpirationIdleTimeAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        regionIdle = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_IDLE,
            regionExpirationIdleTime, regionExpirationIdleTimeAction);
      }
      RegionFunctionArgs.ExpirationAttrs regionTTL = null;
      if (regionExpirationTTL != null || regionExpirationTTLAction != null) {
        if (regionExpirationTTL != null && regionExpirationTTL == -1) {
          regionExpirationTTL = ExpirationAttributes.DEFAULT.getTimeout();
        }
        if (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(regionExpirationTTLAction)) {
          regionExpirationTTLAction = ExpirationAttributes.DEFAULT.getAction().toString();
        }
        regionTTL = new RegionFunctionArgs.ExpirationAttrs(RegionFunctionArgs.ExpirationAttrs.ExpirationFor.REGION_TTL,
            regionExpirationTTL, regionExpirationTTLAction);
      }

      cacheLoader = convertDefaultValue(cacheLoader, StringUtils.EMPTY_STRING);
      cacheWriter = convertDefaultValue(cacheWriter, StringUtils.EMPTY_STRING);

      RegionFunctionArgs regionFunctionArgs = null;
      regionFunctionArgs = new RegionFunctionArgs(regionPath, null, null, false, null, null, null, entryIdle, entryTTL,
        regionIdle, regionTTL, null, null, null, null, cacheListeners, cacheLoader, cacheWriter, asyncEventQueueIds,
        gatewaySenderIds, null, cloningEnabled, null, null, null, null, null, null, null, null, evictionMax, null, null, null);

      Set<String> cacheListenersSet = regionFunctionArgs.getCacheListeners();
      if (cacheListenersSet != null && !cacheListenersSet.isEmpty()) {
        for (String cacheListener : cacheListenersSet) {
          if (!isClassNameValid(cacheListener)) {
            throw new IllegalArgumentException(CliStrings.format(
                CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID,
                new Object[] { cacheListener }));
          }
        }
      }

      if (cacheLoader != null && !isClassNameValid(cacheLoader)) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID, new Object[] { cacheLoader }));
      }

      if (cacheWriter != null && !isClassNameValid(cacheWriter)) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID, new Object[] { cacheWriter }));
      }
              
      if (evictionMax != null && evictionMax < 0) {
        throw new IllegalArgumentException(CliStrings.format(
            CliStrings.ALTER_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_EVICTIONMAX_0_IS_NOT_VALID, new Object[] { evictionMax }));
      }

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(groups, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(new RegionAlterFunction(), regionFunctionArgs, targetMembers);
      List<CliFunctionResult> regionAlterResults = (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult regionAlterResult : regionAlterResults) {
        boolean success  = regionAlterResult.isSuccessful();
        tabularResultData.accumulate("Member", regionAlterResult.getMemberIdOrName());
        if (success) {
          tabularResultData.accumulate("Status", regionAlterResult.getMessage());
          xmlEntity = regionAlterResult.getXmlEntity();
        } else {
          tabularResultData.accumulate("Status", errorPrefix + regionAlterResult.getMessage());
          tabularResultData.setStatus(Status.ERROR);
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, groups));
    }
    return result;
  }
  
  private static boolean regionExists(Cache cache, String regionPath) {
    boolean regionFound = false;
    if (regionPath != null && !Region.SEPARATOR.equals(regionPath)) {
      ManagementService managementService = ManagementService.getExistingManagementService(cache);
      DistributedSystemMXBean dsMBean = managementService.getDistributedSystemMXBean();

      String[] allRegionPaths = dsMBean.listAllRegionPaths();
      for (int i = 0; i < allRegionPaths.length; i++) {
        if (allRegionPaths[i].equals(regionPath)) {
          regionFound = true;
          break;
        }
      }
    }
    return regionFound;
  }
  
  private void validateRegionPathAndParent(Cache cache, String regionPath) {
    if (regionPath == null || "".equals(regionPath)) {
      throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
    }
    // If a region path indicates a sub-region, check whether the parent region exists
    RegionPath regionPathData = new RegionPath(regionPath);
    String parentRegionPath = regionPathData.getParent();
    if (parentRegionPath != null && !Region.SEPARATOR.equals(parentRegionPath)) {
      if (!regionExists(cache, parentRegionPath)) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__PARENT_REGION_FOR_0_DOESNOT_EXIST, new Object[] {regionPath}));
      }
    }
  }


  private void validateGroups(Cache cache, String[] groups) {
    if (groups != null && groups.length != 0) {
      Set<String> existingGroups = new HashSet<String>();
      Set<DistributedMember> members = CliUtil.getAllNormalMembers(cache);
      for (DistributedMember distributedMember : members) {
        List<String> memberGroups = distributedMember.getGroups();
        existingGroups.addAll(memberGroups);
      }
      List<String> groupsList = new ArrayList<String>(Arrays.asList(groups));
      groupsList.removeAll(existingGroups);
  
      if (!groupsList.isEmpty()) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__GROUPS_0_ARE_INVALID, new Object[] {String.valueOf(groupsList)}));
      }
    }
  }

  private void validateRegionFunctionArgs(Cache cache, RegionFunctionArgs regionFunctionArgs) {
    if (regionFunctionArgs.getRegionPath() == null) {
      throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
    }

    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMBean = managementService.getDistributedSystemMXBean();

    String useAttributesFrom = regionFunctionArgs.getUseAttributesFrom();
    if (useAttributesFrom != null && !useAttributesFrom.isEmpty() && regionExists(cache, useAttributesFrom)) {
      if (!regionExists(cache, useAttributesFrom)) { // check already done in createRegion !!!
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND, new Object[] {CliStrings.CREATE_REGION__USEATTRIBUTESFROM, useAttributesFrom}));
      }
      if (!regionFunctionArgs.isSetUseAttributesFrom() || regionFunctionArgs.getRegionAttributes() == null) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_VERIFY_REGION_EXISTS, useAttributesFrom));
      }
    }

    if (regionFunctionArgs.hasPartitionAttributes()) {
      RegionFunctionArgs.PartitionArgs partitionArgs = regionFunctionArgs.getPartitionArgs();
      String colocatedWith = partitionArgs.getPrColocatedWith();
      if (colocatedWith != null && !colocatedWith.isEmpty()) {
        String[] listAllRegionPaths = dsMBean.listAllRegionPaths();
        String foundRegionPath = null;
        for (String regionPath : listAllRegionPaths) {
          if (regionPath.equals(colocatedWith)) {
            foundRegionPath = regionPath;
            break;
          }
        }
        if (foundRegionPath == null) {
          throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND, new Object[] {CliStrings.CREATE_REGION__COLOCATEDWITH, colocatedWith}));
        }
        ManagementService mgmtService = ManagementService.getExistingManagementService(cache);
        DistributedRegionMXBean distributedRegionMXBean = mgmtService.getDistributedRegionMXBean(foundRegionPath);
        String regionType = distributedRegionMXBean.getRegionType();
        if ( ! ( DataPolicy.PARTITION.toString().equals(regionType) || DataPolicy.PERSISTENT_PARTITION.toString().equals(regionType) ) ) {
          throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION, new Object[] {colocatedWith}));
        }
      }
      if (partitionArgs.isSetPRLocalMaxMemory()) {
        int prLocalMaxMemory = partitionArgs.getPrLocalMaxMemory();
        if (prLocalMaxMemory < 0) {
          throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_PARTITIONATTRIBUTES_LOCALMAXMEMORY_MUST_NOT_BE_NEGATIVE.toLocalizedString());
        }
      }
      if (partitionArgs.isSetPRTotalMaxMemory()) {
        long prTotalMaxMemory = partitionArgs.getPrTotalMaxMemory();
        if (prTotalMaxMemory <= 0) {
          throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_TOTAL_SIZE_OF_PARTITION_REGION_MUST_BE_0.toLocalizedString());
        }
      }
      if (partitionArgs.isSetPRRedundantCopies()) {
        int prRedundantCopies = partitionArgs.getPrRedundantCopies();
        switch (prRedundantCopies) {
          case 0:
          case 1:
          case 2:
          case 3:
            break;
          default:
            throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__REDUNDANT_COPIES_SHOULD_BE_ONE_OF_0123, new Object[] {prRedundantCopies}));
        }
      }
//      Validation for the following is not known yet
//      if (partitionArgs.isSetPRTotalNumBuckets()) {
//        int prTotalNumBuckets = partitionArgs.getPrTotalNumBuckets();
//      }
//      if (partitionArgs.isSetPRStartupRecoveryDelay()) {
//        long prStartupRecoveryDelay = partitionArgs.getPrStartupRecoveryDelay();
//      }
//      if (partitionArgs.isSetPRRecoveryDelay()) {
//        long prRecoveryDelay = partitionArgs.getPrRecoveryDelay();
//      }
    }

    String keyConstraint = regionFunctionArgs.getKeyConstraint();
    if (keyConstraint != null && !isClassNameValid(keyConstraint)) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_KEYCONSTRAINT_0_IS_INVALID, new Object[] {keyConstraint}));
    }

    String valueConstraint = regionFunctionArgs.getValueConstraint();
    if (valueConstraint != null && !isClassNameValid(valueConstraint)) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_VALUECONSTRAINT_0_IS_INVALID, new Object[] {valueConstraint}));
    }

    Set<String> cacheListeners = regionFunctionArgs.getCacheListeners();
    if (cacheListeners != null && !cacheListeners.isEmpty()) {
      for (String cacheListener : cacheListeners) {
        if (!isClassNameValid(cacheListener)) {
          throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID, new Object[] {cacheListener}));
        }
      }
    }

    String cacheLoader = regionFunctionArgs.getCacheLoader();
    if (cacheLoader != null && !isClassNameValid(cacheLoader)) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID, new Object[] {cacheLoader}));
    }

    String cacheWriter = regionFunctionArgs.getCacheWriter();
    if (cacheWriter != null && !isClassNameValid(cacheWriter)) {
      throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID, new Object[] {cacheWriter}));
    }

    Set<String> gatewaySenderIds = regionFunctionArgs.getGatewaySenderIds();
    if (gatewaySenderIds != null && !gatewaySenderIds.isEmpty()) {
      String[] gatewaySenders = dsMBean.listGatewaySenders();
      if (gatewaySenders.length == 0) {
        throw new IllegalArgumentException(CliStrings.CREATE_REGION__MSG__NO_GATEWAYSENDERS_IN_THE_SYSTEM);
      } else {
        List<String> gatewaySendersList = new ArrayList<String>(Arrays.asList(gatewaySenders));
        gatewaySenderIds = new HashSet<String>(gatewaySenderIds);
        gatewaySenderIds.removeAll(gatewaySendersList);
        if (!gatewaySenderIds.isEmpty()) {
          throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_GATEWAYSENDER_ID_UNKNOWN_0, new Object[] {gatewaySenderIds}));
        }
      }
    }
    
    if (regionFunctionArgs.isSetConcurrencyLevel()) {
      int concurrencyLevel = regionFunctionArgs.getConcurrencyLevel();
      if (concurrencyLevel < 0) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_CONCURRENCYLEVEL_0_IS_NOT_VALID, new Object[] {concurrencyLevel}));
      }
    }
    
    String diskStore = regionFunctionArgs.getDiskStore();
    if (diskStore != null) {
      RegionShortcut regionShortcut = regionFunctionArgs.getRegionShortcut();
      if (regionShortcut != null && !PERSISTENT_OVERFLOW_SHORTCUTS.contains(regionShortcut)) {
        String subMessage = LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString();
        String message = subMessage + ". " +  CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0, new Object[] {String.valueOf(PERSISTENT_OVERFLOW_SHORTCUTS)});
        
        throw new IllegalArgumentException(message);
      }

      RegionAttributes<?, ?> regionAttributes = regionFunctionArgs.getRegionAttributes();
      if (regionAttributes != null && !regionAttributes.getDataPolicy().withPersistence()) {
        String subMessage = LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString();
        String message = subMessage+ ". " + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FROM_REGION_0_IS_NOT_WITH_PERSISTENCE, 
                                                              new Object[] { String.valueOf(regionFunctionArgs.getUseAttributesFrom()) });

        throw new IllegalArgumentException(message);
      }
      
      if (!diskStoreExists(cache, diskStore)) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0, new Object[] {diskStore}));
      }
    }
    
    RegionFunctionArgs.ExpirationAttrs entryExpirationIdleTime = regionFunctionArgs.getEntryExpirationIdleTime();
    RegionFunctionArgs.ExpirationAttrs entryExpirationTTL = regionFunctionArgs.getEntryExpirationTTL();
    RegionFunctionArgs.ExpirationAttrs regionExpirationIdleTime = regionFunctionArgs.getRegionExpirationIdleTime();
    RegionFunctionArgs.ExpirationAttrs regionExpirationTTL = regionFunctionArgs.getRegionExpirationTTL();
    
    if ((!regionFunctionArgs.isSetStatisticsEnabled() || !regionFunctionArgs.isStatisticsEnabled())
        && (entryExpirationIdleTime != null || entryExpirationTTL != null
            || regionExpirationIdleTime != null || regionExpirationTTL != null)) {
      String message = LocalizedStrings.AttributesFactory_STATISTICS_MUST_BE_ENABLED_FOR_EXPIRATION.toLocalizedString();
      throw new IllegalArgumentException(message + ".");
    }
    
    boolean compressorFailure = false;
    if(regionFunctionArgs.isSetCompressor()) {
      String compressorClassName = regionFunctionArgs.getCompressor();
      Object compressor = null;
      try {
        Class<?> compressorClass = (Class<?>) ClassPathLoader.getLatest().forName(compressorClassName);
        compressor = compressorClass.newInstance();
      } catch (InstantiationException e) {
        compressorFailure = true;
      } catch (IllegalAccessException e) {
        compressorFailure = true;
      } catch (ClassNotFoundException e) {
        compressorFailure = true;
      }
      
      if (compressorFailure || !(compressor instanceof Compressor)) {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__INVALID_COMPRESSOR,
            new Object[] { regionFunctionArgs.getCompressor() }));
      }
    }
  }
  
  private boolean diskStoreExists(Cache cache, String diskStoreName) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();
    
    Set<Entry<String, String[]>> entrySet = diskstore.entrySet();
    
    for (Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (CliUtil.contains(value, diskStoreName)) {
        return true;
      }
    }
    
    return false;
  }

  private static <K, V> FetchRegionAttributesFunctionResult<K, V> getRegionAttributes(Cache cache, String regionPath) {
    if (!isClusterwideSameConfig(cache, regionPath)) {
      throw new IllegalStateException(CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ATTRIBUTES_FORM_REGIONS_EXISTS_BUT_DIFFERENT_SCOPE_OR_DATAPOLICY_USE_DESCRIBE_REGION_FOR_0, regionPath));
    }
    FetchRegionAttributesFunctionResult<K, V> attributes = null;

    // First check whether the region exists on a this manager, if yes then no
    // need to use FetchRegionAttributesFunction to fetch RegionAttributes
    try {
      attributes = FetchRegionAttributesFunction.getRegionAttributes(regionPath);
    } catch (IllegalArgumentException e) {
      /* region doesn't exist on the manager */
    }

    if (attributes == null) {
      // find first member which has the region
      Set<DistributedMember> regionAssociatedMembers = CliUtil.getRegionAssociatedMembers(regionPath, cache, false);
      if (regionAssociatedMembers != null && !regionAssociatedMembers.isEmpty()) {
        DistributedMember distributedMember = regionAssociatedMembers.iterator().next();
        ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(FetchRegionAttributesFunction.INSTANCE, regionPath, distributedMember);
        List<?> resultsList = (List<?>) resultCollector.getResult();
        
        if (resultsList != null && !resultsList.isEmpty()) {
          for (int i = 0; i < resultsList.size(); i++) {
            Object object = resultsList.get(i);
            if (object instanceof IllegalArgumentException) {
              throw (IllegalArgumentException) object;
            } else if (object instanceof Throwable) {
              Throwable th = (Throwable) object;
              LogWrapper.getInstance().info(CliUtil.stackTraceAsString((th)));
              throw new IllegalArgumentException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_REASON_1, new Object[] {regionPath, th.getMessage()}));
            } else { // has to be RegionAttributes
              @SuppressWarnings("unchecked") // to avoid warning :(
              FetchRegionAttributesFunctionResult<K, V> regAttr = ((FetchRegionAttributesFunctionResult<K, V>) object);
              if (attributes == null) {
                attributes = regAttr;
                break;
              } //attributes null check
            }// not IllegalArgumentException or other throwable
          }// iterate over list - there should be only one result in the list
        }// result list is not null or mpty
      }// regionAssociatedMembers is not-empty
    }// attributes are null because do not exist on local member
    
    
    return attributes;
  }

  private static boolean isClusterwideSameConfig(Cache cache, String regionPath) {
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    
    Set<DistributedMember> allMembers = CliUtil.getAllNormalMembers(cache);
    
    RegionAttributesData regionAttributesToValidateAgainst = null;
    for (DistributedMember distributedMember : allMembers) {
      ObjectName regionObjectName;
      try {
        regionObjectName = dsMXBean.fetchRegionObjectName(CliUtil.getMemberNameOrId(distributedMember), regionPath);
        RegionMXBean regionMBean = managementService.getMBeanInstance(regionObjectName, RegionMXBean.class);
        RegionAttributesData regionAttributes = regionMBean.listRegionAttributes();

        if (regionAttributesToValidateAgainst == null) {
          regionAttributesToValidateAgainst = regionAttributes;
        } else if ( !(regionAttributesToValidateAgainst.getScope().equals(regionAttributes.getScope()) ||
            regionAttributesToValidateAgainst.getDataPolicy().equals(regionAttributes.getDataPolicy())) ) {
          return false;
        }
      } catch (Exception e) {
        //ignore
      }
    }

    return true;
  }

  private boolean isClassNameValid(String fqcn) {
    if (fqcn.isEmpty()) {
      return true;
    }
    
    String regex = "([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*";
    return Pattern.matches(regex, fqcn);
  }

  @CliCommand(value = { CliStrings.DESTROY_REGION }, help = CliStrings.DESTROY_REGION__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = CliStrings.TOPIC_GEODE_REGION, writesToSharedConfiguration = true)
  @ResourceOperation(resource=Resource.DATA, operation = Operation.MANAGE)
  public Result destroyRegion(
      @CliOption(key = CliStrings.DESTROY_REGION__REGION,
          optionContext = ConverterHint.REGIONPATH,
          mandatory = true,
          help = CliStrings.DESTROY_REGION__REGION__HELP)
      String regionPath) {

    if (regionPath == null) {
      return ResultBuilder.createInfoResult(CliStrings.DESTROY_REGION__MSG__SPECIFY_REGIONPATH_TO_DESTROY);
    }

    if (regionPath.trim().isEmpty() || regionPath.equals(Region.SEPARATOR)) {
      return ResultBuilder.createInfoResult(
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__REGIONPATH_0_NOT_VALID, new Object[]{regionPath}));
    }

    Result result = null;
    XmlEntity xmlEntity = null;
    try {
      String message = "";
      Cache cache = CacheFactory.getAnyInstance();
      ManagementService managementService = ManagementService.getExistingManagementService(cache);
      String regionPathToUse = regionPath;

      if (!regionPathToUse.startsWith(Region.SEPARATOR)) {
        regionPathToUse = Region.SEPARATOR + regionPathToUse;
      }

      Set<DistributedMember> regionMembersList = findMembersForRegion(cache, managementService, regionPathToUse);

      if (regionMembersList.size() == 0) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.DESTROY_REGION__MSG__COULDNOT_FIND_REGIONPATH_0_IN_GEODE,
                new Object[]{regionPath, "jmx-manager-update-rate milliseconds"}));
      }

      CliFunctionResult destroyRegionResult = null;

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(RegionDestroyFunction.INSTANCE, regionPath,
          regionMembersList);
      List<CliFunctionResult> resultsList = (List<CliFunctionResult>) resultCollector.getResult();
      message = CliStrings.format(CliStrings.DESTROY_REGION__MSG__REGION_0_1_DESTROYED,
          new Object[]{regionPath, /*subRegionMessage*/""});

      // Only if there is an error is this set to false
      boolean isRegionDestroyed = true;
      for (int i = 0; i < resultsList.size(); i++) {
        destroyRegionResult = resultsList.get(i);
        if (destroyRegionResult.isSuccessful()) {
          xmlEntity = destroyRegionResult.getXmlEntity();
        } else if (destroyRegionResult.getThrowable() != null) {
          Throwable t = destroyRegionResult.getThrowable();
          LogWrapper.getInstance().info(t.getMessage(), t);
          message = CliStrings.format(CliStrings.DESTROY_REGION__MSG__ERROR_OCCURRED_WHILE_DESTROYING_0_REASON_1,
              new Object[]{regionPath, t.getMessage()});
          isRegionDestroyed = false;
        } else {
          message = CliStrings.format(CliStrings.DESTROY_REGION__MSG__UNKNOWN_RESULT_WHILE_DESTROYING_REGION_0_REASON_1,
              new Object[]{regionPath, destroyRegionResult.getMessage()});
          isRegionDestroyed = false;
        }
      }
      if (isRegionDestroyed) {
        result = ResultBuilder.createInfoResult(message);
      } else {
        result = ResultBuilder.createUserErrorResult(message);
      }
    } catch (IllegalStateException e) {
      result = ResultBuilder.createUserErrorResult(
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1,
              new Object[]{regionPath, e.getMessage()}));
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1,
              new Object[]{regionPath, e.getMessage()}));
    }

    if (xmlEntity != null) {
      result.setCommandPersisted((new SharedConfigurationWriter()).deleteXmlEntity(xmlEntity, null));
    }
    return result;
  }

  private Set<DistributedMember> findMembersForRegion(Cache cache,
                                                      ManagementService managementService,
                                                      String regionPath) {
    Set<DistributedMember> membersList = new HashSet<>();
    Set<String> regionMemberIds = new HashSet<>();
    MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

    // needs to be escaped with quotes if it contains a hyphen
    if (regionPath.contains("-")) {
      regionPath = "\"" + regionPath + "\"";
    }

    String queryExp = MessageFormat.format(MBeanJMXAdapter.OBJECTNAME__REGION_MXBEAN, regionPath, "*");

    try {
      ObjectName queryExpON = new ObjectName(queryExp);
      Set<ObjectName> queryNames = mbeanServer.queryNames(null, queryExpON);
      if (queryNames == null || queryNames.isEmpty()) {
        return membersList; // protects against null pointer exception below
      }

      boolean addedOneRemote = false;
      for (ObjectName regionMBeanObjectName : queryNames) {
        try {
          RegionMXBean regionMXBean = managementService.getMBeanInstance(regionMBeanObjectName, RegionMXBean.class);
          if (regionMXBean != null) {
            RegionAttributesData regionAttributes = regionMXBean.listRegionAttributes();
            String scope = regionAttributes.getScope();
            // For Scope.LOCAL regions we need to identify each hosting member, but for
            // other scopes we just need a single member as the region destroy will be
            // propagated.
            if (Scope.LOCAL.equals(Scope.fromString(scope))) {
              regionMemberIds.add(regionMXBean.getMember());
            } else {
              if (!addedOneRemote) {
                regionMemberIds.add(regionMXBean.getMember());
                addedOneRemote = true;
              }
            }
          }
        } catch (ClassCastException e) {
          LogWriter logger = cache.getLogger();
          if (logger.finerEnabled()) {
            logger.finer(regionMBeanObjectName + " is not a " + RegionMXBean.class.getSimpleName(), e);
          }
        }
      }

      if (!regionMemberIds.isEmpty()) {
        membersList = getMembersByIds(cache, regionMemberIds);
      }
    } catch (MalformedObjectNameException | NullPointerException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
    }

    return membersList;
  }

  private Set<DistributedMember> getMembersByIds(Cache cache, Set<String> memberIds) {
    Set<DistributedMember> foundMembers = Collections.emptySet();
    if (memberIds != null && !memberIds.isEmpty()) {
      foundMembers = new HashSet<DistributedMember>();
      Set<DistributedMember> allNormalMembers = CliUtil.getAllNormalMembers(cache);

      for (String memberId : memberIds) {
        for (DistributedMember distributedMember : allNormalMembers) {
          if (memberId.equals(distributedMember.getId()) || memberId.equals(distributedMember.getName())) {
            foundMembers.add(distributedMember);
          }
        }
      }
    }
    return foundMembers;
  }

  @CliAvailabilityIndicator({ CliStrings.ALTER_REGION, CliStrings.CREATE_REGION, CliStrings.DESTROY_REGION })
  public boolean isRegionCommandAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) { // in gfsh check if connected //TODO - Abhishek: make this better
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }
}
