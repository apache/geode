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
package org.apache.geode.management.internal.cli.commands;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.geode.GemFireIOException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.BackupStatus;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.lang.ClassUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.PersistentMemberDetails;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.DiskStoreCompacter;
import org.apache.geode.management.internal.cli.util.DiskStoreNotFoundException;
import org.apache.geode.management.internal.cli.util.DiskStoreUpgrader;
import org.apache.geode.management.internal.cli.util.DiskStoreValidater;
import org.apache.geode.management.internal.cli.util.MemberNotFoundException;
import org.apache.geode.management.internal.configuration.SharedConfigurationWriter;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.messages.CompactRequest;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;


/**
 * The DiskStoreCommands class encapsulates all GemFire Disk Store commands in Gfsh.
 * </p>
 * @see org.apache.geode.management.internal.cli.commands.AbstractCommandsSupport
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class DiskStoreCommands extends AbstractCommandsSupport {
  
  @Override
  protected Set<DistributedMember> getMembers(final Cache cache) {
    // TODO determine what this does (as it is untested and unmockable!)
    return CliUtil.getAllMembers(cache);
  }
  
  protected Set<DistributedMember> getNormalMembers(final Cache cache) {
    // TODO determine what this does (as it is untested and unmockable!)
    return CliUtil.getAllNormalMembers(cache);
  }
  
  @CliCommand(value=CliStrings.BACKUP_DISK_STORE, help=CliStrings.BACKUP_DISK_STORE__HELP)
  @CliMetaData(relatedTopic={ CliStrings.TOPIC_GEODE_DISKSTORE })
  @ResourceOperation(resource = Resource.DATA, operation = Operation.READ)
  public Result backupDiskStore(
  
  @CliOption(key=CliStrings.BACKUP_DISK_STORE__DISKDIRS,
  unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
  help=CliStrings.BACKUP_DISK_STORE__DISKDIRS__HELP, 
  mandatory = true)
  String targetDir, 
  @CliOption(key=CliStrings.BACKUP_DISK_STORE__BASELINEDIR,
  help=CliStrings.BACKUP_DISK_STORE__BASELINEDIR__HELP) 
  String baselineDir){

    Result result = null;
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
      DM dm = cache.getDistributionManager();
      BackupStatus backupStatus = null; 
      
      if (baselineDir != null && !baselineDir.isEmpty()) {
       backupStatus = AdminDistributedSystemImpl.backupAllMembers(dm, new File(targetDir), new File(baselineDir));
      } else {
       backupStatus = AdminDistributedSystemImpl.backupAllMembers(dm, new File(targetDir), null);
      }
      
      Map<DistributedMember, Set<PersistentID>> backedupMemberDiskstoreMap = backupStatus.getBackedUpDiskStores();
      
      Set<DistributedMember> backedupMembers = backedupMemberDiskstoreMap.keySet();
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      
      
      if (!backedupMembers.isEmpty()) {
       
        SectionResultData backedupDiskStoresSection = crd.addSection();
        backedupDiskStoresSection.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_BACKED_UP_DISK_STORES);
        TabularResultData backedupDiskStoresTable = backedupDiskStoresSection.addTable();
        
        for (DistributedMember member : backedupMembers) {
          Set<PersistentID> backedupDiskStores =  backedupMemberDiskstoreMap.get(member);
          boolean printMember = true;
          String memberName = member.getName();
          
          if (memberName == null || memberName.isEmpty()) {
            memberName = member.getId();
          }
          for (PersistentID persistentId : backedupDiskStores) {
            if (persistentId != null) {
              
              String UUID = persistentId.getUUID().toString();
              String hostName = persistentId.getHost().getHostName();
              String directory = persistentId.getDirectory();
              
              if (printMember) {
                writeToBackupDisktoreTable(backedupDiskStoresTable, memberName, UUID, hostName, directory);
                printMember = false;
              } else {
                writeToBackupDisktoreTable(backedupDiskStoresTable, "", UUID, hostName, directory);
              }
            }
          }
        }
      } else {
        SectionResultData noMembersBackedUp = crd.addSection();
        noMembersBackedUp.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_NO_DISKSTORES_BACKED_UP);
      }
     
     Set<PersistentID> offlineDiskStores =  backupStatus.getOfflineDiskStores();
     
     if (!offlineDiskStores.isEmpty()) {
       SectionResultData offlineDiskStoresSection = crd.addSection();
       TabularResultData offlineDiskStoresTable = offlineDiskStoresSection.addTable();
       
       offlineDiskStoresSection.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_OFFLINE_DISK_STORES);
       for (PersistentID offlineDiskStore : offlineDiskStores) {
         offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID, offlineDiskStore.getUUID().toString());
         offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST, offlineDiskStore.getHost().getHostName());
         offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY, offlineDiskStore.getDirectory());
       }
     }
     result = ResultBuilder.buildResult(crd);
     
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }
  private void writeToBackupDisktoreTable(TabularResultData backedupDiskStoreTable, String memberId, String UUID, String host, String directory) {
    backedupDiskStoreTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_MEMBER, memberId);
    backedupDiskStoreTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID, UUID);
    backedupDiskStoreTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY, directory);
    backedupDiskStoreTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST, host);
  }


  @CliCommand(value = CliStrings.LIST_DISK_STORE, help = CliStrings.LIST_DISK_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_DISKSTORE })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listDiskStore() {
    try {
      Set<DistributedMember> dataMembers = getNormalMembers(getCache());
      
      if (dataMembers.isEmpty()) {
       return ResultBuilder.createInfoResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }
     
      return toTabularResult(getDiskStoreListing(dataMembers));
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        CliStrings.LIST_DISK_STORE));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.LIST_DISK_STORE__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

  @SuppressWarnings("unchecked")
  protected List<DiskStoreDetails> getDiskStoreListing(Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector = membersFunctionExecutor.execute(new ListDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();
    final List<DiskStoreDetails> distributedSystemMemberDiskStores = new ArrayList<DiskStoreDetails>(results.size());

    for (final Object result : results) {
      if (result instanceof Set) { // ignore FunctionInvocationTargetExceptions and other Exceptions...
        distributedSystemMemberDiskStores.addAll((Set<DiskStoreDetails>) result);
      }
    }

    Collections.sort(distributedSystemMemberDiskStores);

    return distributedSystemMemberDiskStores;
  }

  protected Result toTabularResult(final List<DiskStoreDetails> diskStoreList) throws ResultDataException {
    if (!diskStoreList.isEmpty()) {
      final TabularResultData diskStoreData = ResultBuilder.createTabularResultData();

      for (final DiskStoreDetails diskStoreDetails : diskStoreList) {
        diskStoreData.accumulate("Member Name", diskStoreDetails.getMemberName());
        diskStoreData.accumulate("Member Id", diskStoreDetails.getMemberId());
        diskStoreData.accumulate("Disk Store Name", diskStoreDetails.getName());
        diskStoreData.accumulate("Disk Store ID", diskStoreDetails.getId());
      }

      return ResultBuilder.buildResult(diskStoreData);
    }
    else {
      return ResultBuilder.createInfoResult(CliStrings.LIST_DISK_STORE__DISK_STORES_NOT_FOUND_MESSAGE);
    }
  }

  @CliCommand(value=CliStrings.CREATE_DISK_STORE, help=CliStrings.CREATE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=false, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE}, writesToSharedConfiguration=true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result createDiskStore(@CliOption(key=CliStrings.CREATE_DISK_STORE__NAME,
                                           mandatory=true,
                                           optionContext = ConverterHint.DISKSTORE_ALL, 
                                           help=CliStrings.CREATE_DISK_STORE__NAME__HELP)
                                String name,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION,
                                           specifiedDefaultValue = "true", 
                                           unspecifiedDefaultValue = "false",
                                           help=CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION__HELP)
                                boolean allowForceCompaction,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__AUTO_COMPACT,
                                           specifiedDefaultValue = "true", 
                                           unspecifiedDefaultValue = "true",
                                           help=CliStrings.CREATE_DISK_STORE__AUTO_COMPACT__HELP)
                                boolean autoCompact,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD,
                                           unspecifiedDefaultValue="50",
                                           help=CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD__HELP)
                                int compactionThreshold,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE,
                                           unspecifiedDefaultValue="1024",
                                           help=CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE__HELP)
                                int maxOplogSize,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__QUEUE_SIZE,
                                           unspecifiedDefaultValue="0",
                                           help=CliStrings.CREATE_DISK_STORE__QUEUE_SIZE__HELP)
                                int queueSize,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__TIME_INTERVAL,
                                           unspecifiedDefaultValue="1000",
                                           help=CliStrings.CREATE_DISK_STORE__TIME_INTERVAL__HELP)
                                long timeInterval,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE,
                                           unspecifiedDefaultValue="32768",
                                           help=CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE__HELP)
                                int writeBufferSize,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE,
                                           mandatory=true,
                                           help=CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE__HELP,
                                           optionContext=ConverterHint.STRING_DISABLER)
                                @CliMetaData (valueSeparator = ",")
                                String[] directoriesAndSizes,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__GROUP,
                                           help=CliStrings.CREATE_DISK_STORE__GROUP__HELP,
                                           optionContext=ConverterHint.MEMBERGROUP)
                                @CliMetaData (valueSeparator = ",")
                                String[] groups,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT, 
                                           unspecifiedDefaultValue="90",
                                           help=CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT__HELP)
                                float diskUsageWarningPercentage,
                                @CliOption(key=CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT, 
                                           unspecifiedDefaultValue="99",
                                           help=CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT__HELP)
                                float diskUsageCriticalPercentage) {
    
    try {
      DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
      diskStoreAttributes.allowForceCompaction = allowForceCompaction;
      diskStoreAttributes.autoCompact = autoCompact;
      diskStoreAttributes.compactionThreshold = compactionThreshold;
      diskStoreAttributes.maxOplogSizeInBytes = maxOplogSize * (1024*1024);
      diskStoreAttributes.queueSize = queueSize;
      diskStoreAttributes.timeInterval = timeInterval;
      diskStoreAttributes.writeBufferSize = writeBufferSize;
      
      File[] directories = new File[directoriesAndSizes.length];
      int[] sizes = new int[directoriesAndSizes.length];
      for (int i = 0; i < directoriesAndSizes.length; i++) {
        final int hashPosition = directoriesAndSizes[i].indexOf('#');
        if (hashPosition == -1) {
          directories[i] = new File(directoriesAndSizes[i]);
          sizes[i] = Integer.MAX_VALUE;
        } else {
          directories[i] = new File(directoriesAndSizes[i].substring(0, hashPosition));
          sizes[i] = Integer.parseInt(directoriesAndSizes[i].substring(hashPosition + 1));
        }
      }
      diskStoreAttributes.diskDirs = directories;
      diskStoreAttributes.diskDirSizes = sizes;
      
      diskStoreAttributes.setDiskUsageWarningPercentage(diskUsageWarningPercentage);;
      diskStoreAttributes.setDiskUsageCriticalPercentage(diskUsageCriticalPercentage);

      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(groups, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new CreateDiskStoreFunction(), new Object[] { name, diskStoreAttributes },
          targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      XmlEntity xmlEntity = null;
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
              + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else if (result.isSuccessful()) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;
          
          if (xmlEntity == null) {
            xmlEntity = result.getXmlEntity();
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("Unable to create disk store(s).");
      }
      
      Result result = ResultBuilder.buildResult(tabularData);
      
      if (xmlEntity != null) {
        result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, groups));
      }
      
      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.CREATE_DISK_STORE__ERROR_WHILE_CREATING_REASON_0,
          new Object[] { th.getMessage() }));
    }
  }

    
  @CliCommand(value=CliStrings.COMPACT_DISK_STORE, help=CliStrings.COMPACT_DISK_STORE__HELP)
  @CliMetaData(shellOnly=false, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result compactDiskStore(@CliOption(key=CliStrings.COMPACT_DISK_STORE__NAME,
                                            mandatory=true,
                                            optionContext = ConverterHint.DISKSTORE_ALL, 
                                            help=CliStrings.COMPACT_DISK_STORE__NAME__HELP)
                                 String diskStoreName,
                                 @CliOption(key=CliStrings.COMPACT_DISK_STORE__GROUP,
                                            unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
                                            help=CliStrings.COMPACT_DISK_STORE__GROUP__HELP,
                                            optionContext=ConverterHint.STRING_DISABLER)
                                 @CliMetaData (valueSeparator = ",")
                                 String[] groups) {
    Result result = null;

    try {
      // disk store exists validation
      if (!diskStoreExists(diskStoreName)) {
        result = ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.COMPACT_DISK_STORE__DISKSTORE_0_DOESNOT_EXIST, new Object[] {diskStoreName}));
      } else {
        InternalDistributedSystem ds = (InternalDistributedSystem)getCache().getDistributedSystem();
  
        Map<DistributedMember, PersistentID> overallCompactInfo = new HashMap<DistributedMember, PersistentID>();
        
        Set<?> otherMembers = ds.getDistributionManager().getOtherNormalDistributionManagerIds();
        Set<InternalDistributedMember> allMembers = new HashSet<InternalDistributedMember>();
        
        for (Object member : otherMembers) {
          allMembers.add((InternalDistributedMember) member);
        }
        allMembers.add(ds.getDistributedMember());
        otherMembers = null;
        
        String  groupInfo = "";
        // if groups are specified, find members in the specified group
        if (groups != null && groups.length > 0) {
          groupInfo = CliStrings.format(CliStrings.COMPACT_DISK_STORE__MSG__FOR_GROUP, new Object[] {Arrays.toString(groups) + "."});
          final Set<InternalDistributedMember> selectedMembers = new HashSet<InternalDistributedMember>();
          List<String> targetedGroups = Arrays.asList(groups);
          for (Iterator<InternalDistributedMember> iterator = allMembers.iterator(); iterator.hasNext();) {
            InternalDistributedMember member = iterator.next();
            List<String> memberGroups = member.getGroups();
            if (!Collections.disjoint(targetedGroups, memberGroups)) {
              selectedMembers.add(member);
            }
          }
          
          allMembers = selectedMembers;
        }
        
        // allMembers should not be empty when groups are not specified - it'll
        // have at least one member
        if (allMembers.isEmpty()) {
          result = ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.COMPACT_DISK_STORE__NO_MEMBERS_FOUND_IN_SPECIFED_GROUP, new Object[] {Arrays.toString(groups)}));
        } else {
          // first invoke on local member if it exists in the targeted set
          if (allMembers.remove(ds.getDistributedMember())) {
            PersistentID compactedDiskStoreId = CompactRequest.compactDiskStore(diskStoreName);
            if (compactedDiskStoreId != null) {
              overallCompactInfo.put(ds.getDistributedMember(), compactedDiskStoreId);
            }
          }
          
          // was this local member the only one? Then don't try to send
          // CompactRequest. Otherwise, send the request to others
          if (!allMembers.isEmpty()) {
            // Invoke compact on all 'other' members
            Map<DistributedMember, PersistentID> memberCompactInfo = CompactRequest.send(ds.getDistributionManager(), diskStoreName, allMembers);
            if (memberCompactInfo != null && !memberCompactInfo.isEmpty()) {
              overallCompactInfo.putAll(memberCompactInfo);
              memberCompactInfo.clear();
            }
            String notExecutedMembers = CompactRequest.getNotExecutedMembers();
            LogWrapper.getInstance().info("compact disk-store \""+diskStoreName+"\" message was scheduled to be sent to but was not send to "+notExecutedMembers);
          }
  
          // If compaction happened at all, then prepare the summary
          if (overallCompactInfo != null && !overallCompactInfo.isEmpty()) {
            CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
            SectionResultData section = null;
            
            Set<Entry<DistributedMember, PersistentID>> entries = overallCompactInfo.entrySet();
    
            for (Entry<DistributedMember, PersistentID> entry : entries) {
              String memberId = entry.getKey().getId();
              section = compositeResultData.addSection(memberId);
              section.addData("On Member", memberId);
    
              PersistentID persistentID = entry.getValue();
              if (persistentID != null) {
                SectionResultData subSection = section.addSection("DiskStore"+memberId);
                subSection.addData("UUID", persistentID.getUUID());
                subSection.addData("Host", persistentID.getHost().getHostName());
                subSection.addData("Directory", persistentID.getDirectory());
              }
            }
            compositeResultData.setHeader("Compacted " + diskStoreName + groupInfo);
            result = ResultBuilder.buildResult(compositeResultData);
          } else {
            result = ResultBuilder.createInfoResult(CliStrings.COMPACT_DISK_STORE__COMPACTION_ATTEMPTED_BUT_NOTHING_TO_COMPACT);
          }
        } // all members' if
      } // disk store exists' if
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
      result = ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COMPACT_DISK_STORE__ERROR_WHILE_COMPACTING_REASON_0, new Object[] {e.getMessage()}));
    }
    
    return result;
  }
  
  private boolean diskStoreExists(String diskStoreName) {
    Cache cache = getCache();
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

  @CliCommand(value=CliStrings.COMPACT_OFFLINE_DISK_STORE, help=CliStrings.COMPACT_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE })
  public Result compactOfflineDiskStore(
                 @CliOption(key=CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME,
                            mandatory=true,
                            help=CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME__HELP)
                 String diskStoreName,
                 @CliOption(key=CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS,
                            mandatory=true,
                            unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
                            help=CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS__HELP,
                            optionContext=ConverterHint.DIRS+":"+ConverterHint.STRING_DISABLER)
                 @CliMetaData (valueSeparator = ",")
                 String[] diskDirs,
                 @CliOption(key=CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE,
                            unspecifiedDefaultValue="-1",
                            help=CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE__HELP)
                 long maxOplogSize,
                 @CliOption(key=CliStrings.COMPACT_OFFLINE_DISK_STORE__J,
                            unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
                            help=CliStrings.COMPACT_OFFLINE_DISK_STORE__J__HELP)
                 @CliMetaData (valueSeparator = ",")
                 String[] jvmProps) {
    Result result = null;
    LogWrapper logWrapper = LogWrapper.getInstance();

    StringBuilder output = new StringBuilder();
    StringBuilder error = new StringBuilder();
    String errorMessage = "";
    Process compacterProcess = null;

    try {
      String validatedDirectories = validatedDirectories(diskDirs);
      if (validatedDirectories != null) {
        throw new IllegalArgumentException("Could not find "+CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS + ": \""+validatedDirectories+"\"");
      }

      List<String> commandList = new ArrayList<String>();
      commandList.add(System.getProperty("java.home") + File.separatorChar + "bin"+ File.separatorChar + "java");
      
      configureLogging(commandList);
          
      if (jvmProps != null && jvmProps.length != 0) {
        for (int i = 0; i < jvmProps.length; i++) {
          commandList.add(jvmProps[i]);
        }
      }
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(DiskStoreCompacter.class.getName());
      
      commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME+"="+diskStoreName);
      
      if (diskDirs != null && diskDirs.length != 0) {
        StringBuilder builder = new StringBuilder();
        int arrayLength = diskDirs.length;
        for (int i = 0; i < arrayLength; i++) {
          if (File.separatorChar == '\\') {
            builder.append(diskDirs[i].replace("\\", "/")); // see 46120
          } else {
            builder.append(diskDirs[i]);
          }
          if (i + 1 != arrayLength) {
            builder.append(',');
          }
        }
        commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS+"="+builder.toString());
      }
      // -1 is ignore as maxOplogSize
      commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE+"="+maxOplogSize);

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      compacterProcess = procBuilder.start();
      InputStream inputStream = compacterProcess.getInputStream();
      InputStream errorStream = compacterProcess.getErrorStream();
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(inputStream));
      BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));

      String line = null;
      while ((line = inputReader.readLine()) != null) {
        output.append(line).append(GfshParser.LINE_SEPARATOR);
      }

      line = null;
      boolean switchToStackTrace = false;
      while ((line = errorReader.readLine()) != null) {
        if (!switchToStackTrace && DiskStoreCompacter.STACKTRACE_START.equals(line)) {
          switchToStackTrace = true;
        } else if (switchToStackTrace) {
          error.append(line).append(GfshParser.LINE_SEPARATOR);
        } else {
          errorMessage = errorMessage + line;
        }
      }

      if (!errorMessage.isEmpty()) {
        throw new GemFireIOException(errorMessage);
      }

      // do we have to waitFor??
      compacterProcess.destroy();
      result = ResultBuilder.createInfoResult(output.toString());
    } catch (IOException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      String fieldsMessage = (maxOplogSize != -1 ? CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE + "=" +maxOplogSize + "," : "");
      fieldsMessage += CliUtil.arrayToString(diskDirs);
      String errorString = CliStrings.format(CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2, new Object[] {diskStoreName, fieldsMessage});
      result = ResultBuilder.createUserErrorResult(errorString);
      if (logWrapper.fineEnabled()) {
        logWrapper.fine(e.getMessage(), e);
      }
    } catch (GemFireIOException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      result = ResultBuilder.createUserErrorResult(errorMessage);
      if (logWrapper.fineEnabled()) {
        logWrapper.fine(error.toString());
      }
    } catch (IllegalArgumentException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } finally {
      if (compacterProcess != null) {
        try {
          // just to check whether the process has exited
          // Process.exitValue() throws IllegalThreadStateException if Process 
          // is alive
          compacterProcess.exitValue();
        } catch (IllegalThreadStateException ise) {
          // not yet terminated, destroy the process
          compacterProcess.destroy();
        }
      }
    }
    return result;
  }

  
  @CliCommand(value=CliStrings.UPGRADE_OFFLINE_DISK_STORE, help=CliStrings.UPGRADE_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE })
  public Result upgradeOfflineDiskStore(
      @CliOption(key=CliStrings.UPGRADE_OFFLINE_DISK_STORE__NAME, 
      mandatory=true,
      help=CliStrings.UPGRADE_OFFLINE_DISK_STORE__NAME__HELP)
      String diskStoreName,
      @CliOption(key=CliStrings.UPGRADE_OFFLINE_DISK_STORE__DISKDIRS,
      mandatory=true,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.UPGRADE_OFFLINE_DISK_STORE__DISKDIRS__HELP,
      optionContext=ConverterHint.DIRS+":"+ConverterHint.STRING_DISABLER)
      @CliMetaData (valueSeparator = ",")
      String[] diskDirs,
      @CliOption(key=CliStrings.UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE,
      unspecifiedDefaultValue="-1",
      help=CliStrings.UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE__HELP)
      long maxOplogSize,
      @CliOption(key=CliStrings.UPGRADE_OFFLINE_DISK_STORE__J,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.UPGRADE_OFFLINE_DISK_STORE__J__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] jvmProps) throws InterruptedException {

    
    Result result = null;
    LogWrapper logWrapper = LogWrapper.getInstance();

    StringBuilder output = new StringBuilder();
    StringBuilder error = new StringBuilder();
    String errorMessage = "";
    Process upgraderProcess = null;
      
    try {
      String validatedDirectories = validatedDirectories(diskDirs);
      if (validatedDirectories != null) {
        throw new IllegalArgumentException("Could not find "+CliStrings.UPGRADE_OFFLINE_DISK_STORE__DISKDIRS + ": \""+validatedDirectories+"\"");
      }

      List<String> commandList = new ArrayList<String>();
      commandList.add(System.getProperty("java.home") + File.separatorChar + "bin"+ File.separatorChar + "java");

      configureLogging(commandList);

      if (jvmProps != null && jvmProps.length != 0) {
        for (int i = 0; i < jvmProps.length; i++) {
          commandList.add(jvmProps[i]);
        }
      }
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(DiskStoreUpgrader.class.getName());
      
      commandList.add(CliStrings.UPGRADE_OFFLINE_DISK_STORE__NAME+"="+diskStoreName);
      
      if (diskDirs != null && diskDirs.length != 0) {
        StringBuilder builder = new StringBuilder();
        int arrayLength = diskDirs.length;
        for (int i = 0; i < arrayLength; i++) {
          if (File.separatorChar == '\\') {
            builder.append(diskDirs[i].replace("\\", "/")); // see 46120
          } else {
            builder.append(diskDirs[i]);
          }
          if (i + 1 != arrayLength) {
            builder.append(',');
          }
        }
        commandList.add(CliStrings.UPGRADE_OFFLINE_DISK_STORE__DISKDIRS+"="+builder.toString());
      }
      // -1 is ignore as maxOplogSize
      commandList.add(CliStrings.UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE+"="+maxOplogSize);

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      //procBuilder.redirectErrorStream(true);
      upgraderProcess = procBuilder.start();
      InputStream inputStream = upgraderProcess.getInputStream();
      InputStream errorStream = upgraderProcess.getErrorStream();
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(inputStream));
      BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));

      String line = null;
      while ((line = inputReader.readLine()) != null) {
        output.append(line).append(GfshParser.LINE_SEPARATOR);
      }

      line = null;
      boolean switchToStackTrace = false;
      while ((line = errorReader.readLine()) != null) {
        if (!switchToStackTrace && DiskStoreUpgrader.STACKTRACE_START.equals(line)) {
          switchToStackTrace = true;
        } else if (switchToStackTrace) {
          error.append(line).append(GfshParser.LINE_SEPARATOR);
        } else {
          errorMessage = errorMessage + line;
        }
      }

      if (!errorMessage.isEmpty()) {
        throw new GemFireIOException(errorMessage);
      }

      // do we have to waitFor??
      //upgraderProcess.waitFor();
      upgraderProcess.destroy();
      result = ResultBuilder.createInfoResult(output.toString());
    } catch (IOException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      String fieldsMessage = (maxOplogSize != -1 ? CliStrings.UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE + "=" +maxOplogSize + "," : "");
      fieldsMessage += CliUtil.arrayToString(diskDirs);
      String errorString = CliStrings.format(CliStrings.UPGRADE_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2, new Object[] {diskStoreName, fieldsMessage});
      result = ResultBuilder.createUserErrorResult(errorString);
      if (logWrapper.fineEnabled()) {
        logWrapper.fine(e.getMessage(), e);
      }
    } catch (GemFireIOException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      result = ResultBuilder.createUserErrorResult(errorMessage);
      if (logWrapper.fineEnabled()) {
        logWrapper.fine(error.toString());
      }
    } catch (IllegalArgumentException e) {
      if (output.length() != 0) {
        Gfsh.println(output.toString());
      }
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    } finally {
      if (upgraderProcess != null) {
        try {
          // just to check whether the process has exited
          // Process.exitValue() throws IllegalStateException if Process is alive
          upgraderProcess.exitValue();
        } catch (IllegalThreadStateException itse) {
          // not yet terminated, destroy the process
          upgraderProcess.destroy();
        }
      }
    }
    return result;
  }
  
  
  
  private String validatedDirectories(String[] diskDirs) {
    String invalidDirectories = null;
    StringBuilder builder = null;
    File diskDir = null; 
    for (String diskDirPath : diskDirs) {
      diskDir = new File(diskDirPath);
      if (!diskDir.exists()) {
        if (builder == null) {
          builder = new StringBuilder();
        } else if (builder.length() != 0) {
          builder.append(", ");
        }
        builder.append(diskDirPath);
      }
    }
    if (builder != null) {
      invalidDirectories = builder.toString();
    } 

    return invalidDirectories;
  }

  @CliCommand(value = CliStrings.DESCRIBE_DISK_STORE, help = CliStrings.DESCRIBE_DISK_STORE__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_DISKSTORE })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result describeDiskStore(@CliOption(key = CliStrings.DESCRIBE_DISK_STORE__MEMBER, mandatory = true, optionContext = ConverterHint.MEMBERIDNAME, help = CliStrings.DESCRIBE_DISK_STORE__MEMBER__HELP)
                                  final String memberName,
                                  @CliOption(key = CliStrings.DESCRIBE_DISK_STORE__NAME, mandatory = true, optionContext = ConverterHint.DISKSTORE_ALL, help = CliStrings.DESCRIBE_DISK_STORE__NAME__HELP)
                                  final String diskStoreName) {
    try {
      return toCompositeResult(getDiskStoreDescription(memberName, diskStoreName));
    }
    catch (DiskStoreNotFoundException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    }
    catch (MemberNotFoundException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        CliStrings.DESCRIBE_DISK_STORE));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.DESCRIBE_DISK_STORE__ERROR_MESSAGE,
        memberName, diskStoreName, toString(t, isDebugging())));
    }
  }

  protected DiskStoreDetails getDiskStoreDescription(final String memberName, final String diskStoreName) {
    final DistributedMember member = getMember(getCache(), memberName); // may throw a MemberNotFoundException

    final ResultCollector<?, ?> resultCollector = getMembersFunctionExecutor(Collections.singleton(member))
      .withArgs(diskStoreName).execute(new DescribeDiskStoreFunction());

    final Object result = ((List<?>) resultCollector.getResult()).get(0);

    if (result instanceof DiskStoreDetails) { // disk store details in hand...
      return (DiskStoreDetails) result;
    }
    else if (result instanceof DiskStoreNotFoundException) { // bad disk store name...
      throw (DiskStoreNotFoundException) result;
    }
    else { // unknown and unexpected return type...
      final Throwable cause = (result instanceof Throwable ? (Throwable) result : null);

      if (isLogging()) {
        if (cause != null) {
          getGfsh().logSevere(String.format(
            "Exception (%1$s) occurred while executing '%2$s' on member (%3$s) with disk store (%4$s).",
              ClassUtils.getClassName(cause), CliStrings.DESCRIBE_DISK_STORE, memberName, diskStoreName), cause);
        }
        else {
          getGfsh().logSevere(String.format(
            "Received an unexpected result of type (%1$s) while executing '%2$s' on member (%3$s) with disk store (%4$s).",
              ClassUtils.getClassName(result), CliStrings.DESCRIBE_DISK_STORE, memberName, diskStoreName), null);
        }
      }

      throw new RuntimeException(CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
        ClassUtils.getClassName(result), CliStrings.DESCRIBE_DISK_STORE), cause);
    }
  }

  protected Result toCompositeResult(final DiskStoreDetails diskStoreDetails) {
    final CompositeResultData diskStoreData = ResultBuilder.createCompositeResultData();

    final CompositeResultData.SectionResultData diskStoreSection = diskStoreData.addSection();

    diskStoreSection.addData("Disk Store ID", diskStoreDetails.getId());
    diskStoreSection.addData("Disk Store Name", diskStoreDetails.getName());
    diskStoreSection.addData("Member ID", diskStoreDetails.getMemberId());
    diskStoreSection.addData("Member Name", diskStoreDetails.getMemberName());
    diskStoreSection.addData("Allow Force Compaction", toString(diskStoreDetails.isAllowForceCompaction(), "Yes", "No"));
    diskStoreSection.addData("Auto Compaction", toString(diskStoreDetails.isAutoCompact(), "Yes", "No"));
    diskStoreSection.addData("Compaction Threshold", diskStoreDetails.getCompactionThreshold());
    diskStoreSection.addData("Max Oplog Size", diskStoreDetails.getMaxOplogSize());
    diskStoreSection.addData("Queue Size", diskStoreDetails.getQueueSize());
    diskStoreSection.addData("Time Interval", diskStoreDetails.getTimeInterval());
    diskStoreSection.addData("Write Buffer Size", diskStoreDetails.getWriteBufferSize());
    diskStoreSection.addData("Disk Usage Warning Percentage", diskStoreDetails.getDiskUsageWarningPercentage());
    diskStoreSection.addData("Disk Usage Critical Percentage", diskStoreDetails.getDiskUsageCriticalPercentage());
    diskStoreSection.addData("PDX Serialization Meta-Data Stored",
      toString(diskStoreDetails.isPdxSerializationMetaDataStored(), "Yes", "No"));

    final TabularResultData diskDirTable = diskStoreData.addSection().addTable();

    for (DiskStoreDetails.DiskDirDetails diskDirDetails : diskStoreDetails) {
      diskDirTable.accumulate("Disk Directory", diskDirDetails.getAbsolutePath());
      diskDirTable.accumulate("Size", diskDirDetails.getSize());
    }

    final TabularResultData regionTable = diskStoreData.addSection().addTable();

    for (DiskStoreDetails.RegionDetails regionDetails : diskStoreDetails.iterateRegions()) {
      regionTable.accumulate("Region Path", regionDetails.getFullPath());
      regionTable.accumulate("Region Name", regionDetails.getName());
      regionTable.accumulate("Persistent", toString(regionDetails.isPersistent(), "Yes", "No"));
      regionTable.accumulate("Overflow To Disk", toString(regionDetails.isOverflowToDisk(), "Yes", "No"));
    }

    final TabularResultData cacheServerTable = diskStoreData.addSection().addTable();

    for (DiskStoreDetails.CacheServerDetails cacheServerDetails : diskStoreDetails.iterateCacheServers()) {
      cacheServerTable.accumulate("Bind Address", cacheServerDetails.getBindAddress());
      cacheServerTable.accumulate("Hostname for Clients", cacheServerDetails.getHostName());
      cacheServerTable.accumulate("Port", cacheServerDetails.getPort());
    }

    final TabularResultData gatewayTable = diskStoreData.addSection().addTable();

    for (DiskStoreDetails.GatewayDetails gatewayDetails : diskStoreDetails.iterateGateways()) {
      gatewayTable.accumulate("Gateway ID", gatewayDetails.getId());
      gatewayTable.accumulate("Persistent", toString(gatewayDetails.isPersistent(), "Yes", "No"));
    }

    final TabularResultData asyncEventQueueTable = diskStoreData.addSection().addTable();

    for (DiskStoreDetails.AsyncEventQueueDetails asyncEventQueueDetails : diskStoreDetails.iterateAsyncEventQueues()) {
      asyncEventQueueTable.accumulate("Async Event Queue ID", asyncEventQueueDetails.getId());
    }

    return ResultBuilder.buildResult(diskStoreData);
  }

  @CliCommand(value = CliStrings.REVOKE_MISSING_DISK_STORE, help = CliStrings.REVOKE_MISSING_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_DISKSTORE })
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result revokeMissingDiskStore(
      @CliOption(key = CliStrings.REVOKE_MISSING_DISK_STORE__ID, mandatory = true, help = CliStrings.REVOKE_MISSING_DISK_STORE__ID__HELP)
      String id) {

    try {
      DistributedSystemMXBean dsMXBean = ManagementService.getManagementService(CacheFactory.getAnyInstance())
          .getDistributedSystemMXBean();
      if (dsMXBean.revokeMissingDiskStores(id)) {
        return ResultBuilder.createInfoResult("Missing disk store successfully revoked");
      }

      return ResultBuilder.createUserErrorResult("Unable to find missing disk store to revoke");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (th.getMessage() == null) {
        return ResultBuilder.createGemFireErrorResult("An error occurred while revoking missing disk stores: " + th);
      }
      return ResultBuilder.createGemFireErrorResult("An error occurred while revoking missing disk stores: " + th.getMessage());
    }
  }

  @CliCommand(value = CliStrings.SHOW_MISSING_DISK_STORE, help = CliStrings.SHOW_MISSING_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_DISKSTORE })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result showMissingDiskStore() {

    try {
      Set<DistributedMember> dataMembers = getNormalMembers(getCache());

      if (dataMembers.isEmpty()) {
        return ResultBuilder.createInfoResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }
      List<Object> results = getMissingDiskStoresList(dataMembers);
      return toMissingDiskStoresTabularResult(results);
    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
          CliStrings.SHOW_MISSING_DISK_STORE));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
    if (t.getMessage() == null) {
      return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.SHOW_MISSING_DISK_STORE__ERROR_MESSAGE, t));
    }
    return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.SHOW_MISSING_DISK_STORE__ERROR_MESSAGE, t.getMessage()));
    }
  }

  protected List<Object> getMissingDiskStoresList(Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector = membersFunctionExecutor.execute(new ShowMissingDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();
    final List<Object> distributedPersistentRecoveryDetails = new ArrayList<Object>(results.size());
    for (final Object result: results) {
      if (result instanceof Set) { // ignore FunctionInvocationTargetExceptions and other Exceptions...
        distributedPersistentRecoveryDetails.addAll((Set<Object>) result);
      }
    }
    return distributedPersistentRecoveryDetails;
  }

  protected Result toMissingDiskStoresTabularResult(final List<Object> resultDetails) throws ResultDataException {
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    List<PersistentMemberPattern> missingDiskStores = new ArrayList<PersistentMemberPattern>();
    List<ColocatedRegionDetails> missingColocatedRegions = new ArrayList<ColocatedRegionDetails>();

    for (Object detail : resultDetails) {
      if (detail instanceof PersistentMemberPattern) {
        missingDiskStores.add((PersistentMemberPattern) detail);
      } else if (detail instanceof ColocatedRegionDetails) {
        missingColocatedRegions.add((ColocatedRegionDetails) detail);
      } else {
        throw new ResultDataException("Unknown type of PersistentRecoveryFailures result");
      }
    }

    boolean hasMissingDiskStores = !missingDiskStores.isEmpty();
    boolean hasMissingColocatedRegions = !missingColocatedRegions.isEmpty();
    if (hasMissingDiskStores) {
      SectionResultData missingDiskStoresSection = crd.addSection();
      missingDiskStoresSection.setHeader("Missing Disk Stores");
      TabularResultData missingDiskStoreData = missingDiskStoresSection.addTable();

      for (PersistentMemberPattern peristentMemberDetails : missingDiskStores) {
        missingDiskStoreData.accumulate("Disk Store ID", peristentMemberDetails.getUUID());
        missingDiskStoreData.accumulate("Host", peristentMemberDetails.getHost());
        missingDiskStoreData.accumulate("Directory", peristentMemberDetails.getDirectory());
      }
    } else {
      SectionResultData noMissingDiskStores = crd.addSection();
      noMissingDiskStores.setHeader("No missing disk store found");
    }
    if (hasMissingDiskStores || hasMissingColocatedRegions) {
      // For clarity, separate disk store and colocated region information
      crd.addSection().setHeader("\n");
    }

    if (hasMissingColocatedRegions) {
      SectionResultData missingRegionsSection = crd.addSection();
      missingRegionsSection.setHeader("Missing Colocated Regions");
      TabularResultData missingRegionData = missingRegionsSection.addTable();

      for (ColocatedRegionDetails colocatedRegionDetails:missingColocatedRegions) {
        missingRegionData.accumulate("Host", colocatedRegionDetails.getHost());
        missingRegionData.accumulate("Distributed Member", colocatedRegionDetails.getMember());
        missingRegionData.accumulate("Parent Region", colocatedRegionDetails.getParent());
        missingRegionData.accumulate("Missing Colocated Region", colocatedRegionDetails.getChild());
      }
    } else {
      SectionResultData noMissingColocatedRegions = crd.addSection();
      noMissingColocatedRegions.setHeader("No missing colocated region found");
    }

    return ResultBuilder.buildResult(crd);
  }

  @CliCommand(value=CliStrings.DESCRIBE_OFFLINE_DISK_STORE, help=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE })
  public Result describeOfflineDiskStore(
      @CliOption (key=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME, 
          mandatory=true,
          help=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME__HELP)
        String diskStoreName,
      @CliOption (key=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS,
          mandatory=true,
          help=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS__HELP)
      @CliMetaData (valueSeparator = ",")
        String[] diskDirs,
      @CliOption (key=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES__HELP)
      Boolean listPdxTypes,
      @CliOption  (key=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME, 
          help=CliStrings.DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME__HELP,
          unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE)
        String regionName) {

    try {
      final File[] dirs = new File[diskDirs.length];
      for (int i = 0; i < diskDirs.length; i++) {
        dirs[i] = new File((diskDirs[i]));
      }
      
      if (Region.SEPARATOR.equals(regionName)) {
        return ResultBuilder.createUserErrorResult(CliStrings.INVALID_REGION_NAME);
      }
      
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(outputStream);
      
      DiskStoreImpl.dumpInfo(printStream, diskStoreName, dirs, regionName, listPdxTypes);
      return ResultBuilder.createInfoResult(outputStream.toString());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (th.getMessage() == null) {
        return ResultBuilder.createGemFireErrorResult("An error occurred while describing offline disk stores: " + th);
      }
      return ResultBuilder.createGemFireErrorResult("An error occurred while describing offline disk stores: " + th.getMessage());
    }
  }
  
  @CliCommand(value=CliStrings.EXPORT_OFFLINE_DISK_STORE, help=CliStrings.EXPORT_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE })
  public Result exportOfflineDiskStore(
      @CliOption (key=CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME, 
          mandatory=true,
          help=CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME__HELP)
        String diskStoreName,
      @CliOption (key=CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKDIRS,
          mandatory=true,
          help=CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKDIRS__HELP)
      @CliMetaData (valueSeparator = ",")
        String[] diskDirs,
      @CliOption  (key=CliStrings.EXPORT_OFFLINE_DISK_STORE__DIR,
          mandatory=true,
          help=CliStrings.EXPORT_OFFLINE_DISK_STORE__DIR__HELP)         
        String dir) {

    try {
      final File[] dirs = new File[diskDirs.length];
      for (int i = 0; i < diskDirs.length; i++) {
        dirs[i] = new File((diskDirs[i]));
      }
      
      File output = new File(dir);
      
      //Note, this can consume a lot of memory, so this should
      //not be moved to a separate process unless we provide a way for the user
      //to configure the size of that process.
      DiskStoreImpl.exportOfflineSnapshot(diskStoreName, dirs, output);
      String resultString = CliStrings.format(CliStrings.EXPORT_OFFLINE_DISK_STORE__SUCCESS,diskStoreName, dir);
      return ResultBuilder.createInfoResult(resultString.toString());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      LogWrapper.getInstance().warning(th.getMessage(), th);
      return ResultBuilder
          .createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_OFFLINE_DISK_STORE__ERROR,diskStoreName, th.toString()));
    }
  }
  
  private void configureLogging(final List<String> commandList) {
    URL configUrl = LogService.class.getResource(LogService.CLI_CONFIG);
    String configFilePropertyValue = configUrl.toString();
    commandList.add("-Dlog4j.configurationFile=" + configFilePropertyValue);
  }

  @CliCommand(value=CliStrings.VALIDATE_DISK_STORE, help=CliStrings.VALIDATE_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE }) //offline command
  public Result validateDiskStore(
      @CliOption(key=CliStrings.VALIDATE_DISK_STORE__NAME, mandatory=true,
                  help=CliStrings.VALIDATE_DISK_STORE__NAME__HELP)
                  String diskStoreName,
      @CliOption(key=CliStrings.VALIDATE_DISK_STORE__DISKDIRS,
                  mandatory=true,
                  unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,                 
                  help=CliStrings.VALIDATE_DISK_STORE__DISKDIRS__HELP)
      @CliMetaData (valueSeparator = ",")
                  String[] diskDirs,
      @CliOption(key=CliStrings.VALIDATE_DISK_STORE__J,
                  unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
                  help=CliStrings.VALIDATE_DISK_STORE__J__HELP)
      @CliMetaData (valueSeparator = ",")
      String[] jvmProps) {   
    try {
      String resultString = new String();
      
      // create a new process ...bug 46075
      StringBuilder dirList = new StringBuilder();
      for (int i = 0; i < diskDirs.length; i++) {
        dirList.append(diskDirs[i]);
        dirList.append(";");
      }

      List<String> commandList = new ArrayList<String>();
      commandList.add(System.getProperty("java.home") + File.separatorChar
          + "bin" + File.separatorChar + "java");

      configureLogging(commandList);
      
      if (jvmProps != null && jvmProps.length != 0) {
        for (int i = 0; i < jvmProps.length; i++) {
          commandList.add(jvmProps[i]);
        }
      }
      
      //Pass any java options on to the command
      String opts = System.getenv("JAVA_OPTS");
      if (opts != null) {
        commandList.add(opts);
      }
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(DiskStoreValidater.class.getName());
      commandList.add(diskStoreName);
      commandList.add(dirList.toString());

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      StringBuilder output = new StringBuilder();
      String errorString = new String();

      Process validateDiskStoreProcess = procBuilder.redirectErrorStream(true).start();
      InputStream inputStream = validateDiskStoreProcess.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;

      while ((line = br.readLine()) != null) {
        output.append(line).append(GfshParser.LINE_SEPARATOR);
      }
      validateDiskStoreProcess.destroy();

      if (errorString != null) {
        output.append(errorString).append(GfshParser.LINE_SEPARATOR);
      }
      resultString = "Validating " + diskStoreName + GfshParser.LINE_SEPARATOR + output.toString();
      return ResultBuilder.createInfoResult(resultString.toString());
    } catch (IOException ex) {
      return ResultBuilder
          .createGemFireErrorResult(CliStrings.format(CliStrings.VALIDATE_DISK_STORE__MSG__IO_ERROR,diskStoreName, ex.getMessage() ));
    } catch (Exception ex) {
//      StringPrintWriter s = new StringPrintWriter();
//      ex.printStackTrace(s);
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.VALIDATE_DISK_STORE__MSG__ERROR,diskStoreName, ex.getMessage()));
    }

  }
  
  
  @CliCommand(value=CliStrings.ALTER_DISK_STORE, help=CliStrings.ALTER_DISK_STORE__HELP)
  @CliMetaData(shellOnly=true, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE })
  public Result alterOfflineDiskStore(
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__DISKSTORENAME, 
      mandatory=true,
      help=CliStrings.ALTER_DISK_STORE__DISKSTORENAME__HELP)
      String diskStoreName,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__REGIONNAME, 
      mandatory=true,
      help=CliStrings.ALTER_DISK_STORE__REGIONNAME__HELP)
      String regionName, 
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__DISKDIRS,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__DISKDIRS__HELP,
      mandatory=true)
      @CliMetaData (valueSeparator = ",")
      String[] diskDirs,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__COMPRESSOR,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      specifiedDefaultValue="none",
      help=CliStrings.ALTER_DISK_STORE__COMPRESSOR__HELP)
      String compressorClassName,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL__HELP)
      Integer concurrencyLevel,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED__HELP)
      Boolean statisticsEnabled, 
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY__HELP)
      Integer initialCapacity, 
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__LOAD__FACTOR,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__LOAD__FACTOR__HELP)
      Float loadFactor,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION,
      help=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION__HELP)
      String lruEvictionAction,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM,
      help=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM__HELP)
      String lruEvictionAlgo,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__LIMIT,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__LRU__EVICTION__LIMIT__HELP)
      Integer lruEvictionLimit,
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__OFF_HEAP,
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help=CliStrings.ALTER_DISK_STORE__OFF_HEAP__HELP)
      Boolean offHeap, 
      @CliOption  (key=CliStrings.ALTER_DISK_STORE__REMOVE,
      help=CliStrings.ALTER_DISK_STORE__REMOVE__HELP,
      mandatory = false,
      specifiedDefaultValue = "true",
      unspecifiedDefaultValue = "false")
      boolean remove) {
    
    Result result = null;
    
    try {
      File[] dirs = null;
      
      if (diskDirs != null) {
        dirs = new File[diskDirs.length];
        for (int i=0; i < diskDirs.length; i++) {
          dirs[i] = new File((diskDirs[i]));
        }
      }
      
      if (regionName.equals(Region.SEPARATOR)) {
        return ResultBuilder.createUserErrorResult(CliStrings.INVALID_REGION_NAME);
      }

      if ((lruEvictionAlgo != null) ||
          (lruEvictionAction != null) ||
          (lruEvictionLimit != null) ||
          (concurrencyLevel != null) ||
          (initialCapacity != null) ||
          (loadFactor != null) ||
          (compressorClassName != null) ||
          (offHeap != null) ||
          (statisticsEnabled != null)
          ) {
        if (!remove) {
          String lruEvictionLimitString = lruEvictionLimit == null ? null : lruEvictionLimit.toString();
          String concurrencyLevelString = concurrencyLevel == null ? null : concurrencyLevel.toString();
          String initialCapacityString = initialCapacity == null ? null : initialCapacity.toString();
          String loadFactorString = loadFactor == null ? null : loadFactor.toString();
          String statisticsEnabledString = statisticsEnabled == null ? null : statisticsEnabled.toString();
          String offHeapString = offHeap == null ? null : offHeap.toString();
          
          if ("none".equals(compressorClassName)) {
            compressorClassName = "";
          }
          
          String resultMessage = DiskStoreImpl.modifyRegion(diskStoreName, dirs, "/"+regionName,
              lruEvictionAlgo, lruEvictionAction, lruEvictionLimitString,
              concurrencyLevelString, initialCapacityString, loadFactorString,
              compressorClassName, statisticsEnabledString, offHeapString, false);

          result = ResultBuilder.createInfoResult(resultMessage);
        } else {
          result = ResultBuilder.createParsingErrorResult("Cannot use the --remove=true parameter with any other parameters");
        }
      } else {
        if (remove) {
          DiskStoreImpl.destroyRegion(diskStoreName, dirs, "/" + regionName);
          result = ResultBuilder.createInfoResult("The region " + regionName + " was successfully removed from the disk store " + diskStoreName);
        } else {
          //Please provide an option
          result = ResultBuilder.createParsingErrorResult("Please provide a relevant parameter");
        }
      }
      //Catch the IllegalArgumentException thrown by the modifyDiskStore function and sent the 
    } catch (IllegalArgumentException e) {
      String message = "Please check the parameters";
      message += "\n" + e.getMessage();
      result = ResultBuilder.createGemFireErrorResult(message);
    } catch (IllegalStateException e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (CacheExistsException e) {
      //Indicates that the command is being used when a cache is open 
      result = ResultBuilder.createGemFireErrorResult("Cannot execute " + CliStrings.ALTER_DISK_STORE + " when a cache exists (Offline command)");
    } catch (Exception e) {
      result = createErrorResult(e.getMessage());
    } 
    return result;
  }

  @CliCommand(value=CliStrings.DESTROY_DISK_STORE, help=CliStrings.DESTROY_DISK_STORE__HELP)
  @CliMetaData(shellOnly=false, relatedTopic={CliStrings.TOPIC_GEODE_DISKSTORE}, writesToSharedConfiguration=true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result destroyDiskStore(
      @CliOption  (key=CliStrings.DESTROY_DISK_STORE__NAME, 
          mandatory=true,
          help=CliStrings.DESTROY_DISK_STORE__NAME__HELP)
        String name,
      @CliOption(key=CliStrings.DESTROY_DISK_STORE__GROUP,
          help=CliStrings.DESTROY_DISK_STORE__GROUP__HELP,
          optionContext=ConverterHint.MEMBERGROUP)
        @CliMetaData (valueSeparator = ",")
        String[] groups) {
    try {      
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(groups, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new DestroyDiskStoreFunction(), new Object[] { name },
          targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      XmlEntity xmlEntity = null;
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
              + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else if (result.getMessage() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;
          
          if (xmlEntity == null) {
            xmlEntity = result.getXmlEntity();
          }
        }
      }
      

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("No matching disk stores found.");
      }
      
      Result result = ResultBuilder.buildResult(tabularData);
      if (xmlEntity != null) {
        result.setCommandPersisted((new SharedConfigurationWriter()).deleteXmlEntity(xmlEntity, groups));
      }
      
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.DESTROY_DISK_STORE__ERROR_WHILE_DESTROYING_REASON_0,
          new Object[] { th.getMessage() }));
    }
 }
      
  private Result createErrorResult(String message) {
    ErrorResultData erd = ResultBuilder.createErrorResultData();
    erd.addLine(message);
    return ResultBuilder.buildResult(erd);
  }

  @CliAvailabilityIndicator({CliStrings.BACKUP_DISK_STORE, CliStrings.COMPACT_DISK_STORE,
    CliStrings.DESCRIBE_DISK_STORE, CliStrings.LIST_DISK_STORE, CliStrings.REVOKE_MISSING_DISK_STORE,
    CliStrings.SHOW_MISSING_DISK_STORE, CliStrings.CREATE_DISK_STORE, CliStrings.DESTROY_DISK_STORE})
  public boolean diskStoreCommandsAvailable() {
    // these disk store commands are always available in GemFire
    return (!CliUtil.isGfshVM() || (getGfsh() != null && getGfsh().isConnectedAndReady()));
  }

  @CliAvailabilityIndicator({CliStrings.DESCRIBE_OFFLINE_DISK_STORE})
  public boolean offlineDiskStoreCommandsAvailable() {
    return true;
  }
}
