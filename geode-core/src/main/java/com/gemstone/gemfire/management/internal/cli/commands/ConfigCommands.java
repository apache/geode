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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.AbstractCliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.domain.MemberConfigurationInfo;
import com.gemstone.gemfire.management.internal.cli.functions.AlterRuntimeConfigFunction;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.ExportConfigFunction;
import com.gemstone.gemfire.management.internal.cli.functions.GetMemberConfigInformationFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/****
 * @since GemFire 7.0
 *
 */
public class ConfigCommands implements CommandMarker {
  private final ExportConfigFunction exportConfigFunction = new ExportConfigFunction();
  private final GetMemberConfigInformationFunction getMemberConfigFunction = new GetMemberConfigInformationFunction();
  private final AlterRuntimeConfigFunction alterRunTimeConfigFunction = new AlterRuntimeConfigFunction();
  
  private static Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @CliCommand(value = { CliStrings.DESCRIBE_CONFIG }, help = CliStrings.DESCRIBE_CONFIG__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation= Operation.READ)
  public Result describeConfig(
      @CliOption (key = CliStrings.DESCRIBE_CONFIG__MEMBER,
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help = CliStrings.DESCRIBE_CONFIG__MEMBER__HELP,
      mandatory = true)

      String memberNameOrId,
      @CliOption (key = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS,
      help = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS__HELP,
      unspecifiedDefaultValue="true",
      specifiedDefaultValue="true")
      boolean hideDefaults) {

    Result result = null;
    try {
      DistributedMember targetMember = null;

      if (memberNameOrId != null && !memberNameOrId.isEmpty()) {
        targetMember = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);
      }
      if (targetMember != null) {
        ResultCollector<?,?> rc = CliUtil.executeFunction(getMemberConfigFunction, new Boolean(hideDefaults), targetMember);
        ArrayList<?> output = (ArrayList<?>) rc.getResult();
        Object obj = output.get(0);

        if (obj != null && obj instanceof MemberConfigurationInfo) {
          MemberConfigurationInfo memberConfigInfo = (MemberConfigurationInfo) obj;

          CompositeResultData crd = ResultBuilder.createCompositeResultData();
          crd.setHeader(CliStrings.format(CliStrings.DESCRIBE_CONFIG__HEADER__TEXT, memberNameOrId));

          List<String> jvmArgsList = memberConfigInfo.getJvmInputArguments();
          TabularResultData jvmInputArgs = crd.addSection().addSection().addTable();

          for (String jvmArg : jvmArgsList) {
            jvmInputArgs.accumulate("JVM command line arguments", jvmArg);
          }

          addSection(crd, memberConfigInfo.getGfePropsSetUsingApi(), "GemFire properties defined using the API");
          addSection(crd, memberConfigInfo.getGfePropsRuntime(), "GemFire properties defined at the runtime");
          addSection(crd, memberConfigInfo.getGfePropsSetFromFile(), "GemFire properties defined with the property file");
          addSection(crd, memberConfigInfo.getGfePropsSetWithDefaults(), "GemFire properties using default values");
          addSection(crd, memberConfigInfo.getCacheAttributes(), "Cache attributes");

          List<Map<String, String>> cacheServerAttributesList = memberConfigInfo.getCacheServerAttributes();

          if (cacheServerAttributesList != null && !cacheServerAttributesList.isEmpty()) {
            SectionResultData cacheServerSection = crd.addSection();
            cacheServerSection.setHeader("Cache-server attributes");

            Iterator<Map<String, String>> iters = cacheServerAttributesList.iterator();

            while (iters.hasNext()) {
              Map<String, String> cacheServerAttributes = iters.next();
              addSubSection(cacheServerSection, cacheServerAttributes, "");
            }
          }
          result = ResultBuilder.buildResult(crd);
        }

      } else {
        ErrorResultData erd = ResultBuilder.createErrorResultData();
        erd.addLine(CliStrings.format(CliStrings.DESCRIBE_CONFIG__MEMBER__NOT__FOUND, new Object[] {memberNameOrId}));
        result = ResultBuilder.buildResult(erd);
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.DESCRIBE_CONFIG));
    } catch (Exception e) {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      erd.addLine(e.getMessage());
      result = ResultBuilder.buildResult(erd);
    }
    return result;
  }


  private void addSection(CompositeResultData crd, Map<String, String> attrMap, String headerText) {
    if (attrMap != null && !attrMap.isEmpty()) {
      SectionResultData section = crd.addSection();
      section.setHeader(headerText);
      section.addSeparator('.');
      Set<String> attributes = new TreeSet<String>(attrMap.keySet());

      for (String attribute : attributes) {
        String attributeValue = attrMap.get(attribute);
        section.addData(attribute, attributeValue);
      }
    }
  }

  private void addSubSection (SectionResultData section, Map<String, String> attrMap, String headerText) {
    if (!attrMap.isEmpty()) {
      SectionResultData subSection = section.addSection();
      Set<String> attributes = new TreeSet<String>(attrMap.keySet());
      subSection.setHeader(headerText);

      for (String attribute : attributes) {
        String attributeValue = attrMap.get(attribute);
        subSection.addData(attribute, attributeValue);
      }
    }
  }
  /**
   * Export the cache configuration in XML format.
   *
   * @param member
   *          Member for which to write the configuration
   * @param group
   *          Group or groups for which to write the configuration
   * @return Results of the attempt to write the configuration
   */
  @CliCommand(value = { CliStrings.EXPORT_CONFIG }, help = CliStrings.EXPORT_CONFIG__HELP)
  @CliMetaData(interceptor = "com.gemstone.gemfire.management.internal.cli.commands.ConfigCommands$Interceptor", relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportConfig(
      @CliOption(key = { CliStrings.EXPORT_CONFIG__MEMBER },
                 optionContext = ConverterHint.ALL_MEMBER_IDNAME,
                 help = CliStrings.EXPORT_CONFIG__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",")
                  String member,
      @CliOption(key = { CliStrings.EXPORT_CONFIG__GROUP },
                 optionContext = ConverterHint.MEMBERGROUP,
                 help = CliStrings.EXPORT_CONFIG__GROUP__HELP)
      @CliMetaData (valueSeparator = ",")
                  String group,
      @CliOption(key = { CliStrings.EXPORT_CONFIG__DIR },
                 help = CliStrings.EXPORT_CONFIG__DIR__HELP)
                  String dir) {
    InfoResultData infoData = ResultBuilder.createInfoResultData();

    Set<DistributedMember> targetMembers;
    try {
      targetMembers = CliUtil.findAllMatchingMembers(group, member);
    } catch (CommandResultException crex) {
      return crex.getResult();
    }

    try {
      ResultCollector<?, ?> rc = CliUtil.executeFunction(this.exportConfigFunction, null, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          infoData.addLine(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__EXCEPTION, result.getMemberIdOrName(), result
              .getThrowable()));
        } else if (result.isSuccessful()) {
          String cacheFileName = result.getMemberIdOrName() + "-cache.xml";
          String propsFileName = result.getMemberIdOrName() + "-gf.properties";
          String[] fileContent = (String[]) result.getSerializables();
          infoData.addAsFile(cacheFileName, fileContent[0], "Downloading Cache XML file: {0}", false);
          infoData.addAsFile(propsFileName, fileContent[1], "Downloading properties file: {0}", false);
        }
      }
      return ResultBuilder.buildResult(infoData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      th.printStackTrace(System.err);
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__EXCEPTION, th.getClass()
          .getName()
          + ": " + th.getMessage()));
    }
  }


  @CliCommand(value = { CliStrings.ALTER_RUNTIME_CONFIG }, help = CliStrings.ALTER_RUNTIME_CONFIG__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public Result alterRuntimeConfig(
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__MEMBER},
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER__HELP) String memberNameOrId,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__GROUP},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER__HELP) String group,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT__HELP) Integer archiveDiskSpaceLimit,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT__HELP) Integer archiveFileSizeLimit,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT__HELP) Integer logDiskSpaceLimit,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT__HELP) Integer logFileSizeLimit,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL},
      optionContext = ConverterHint.LOG_LEVEL,
      help = CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL__HELP ) String logLevel,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE},
      help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE__HELP) String statisticArchiveFile,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE__HELP) Integer statisticSampleRate,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED__HELP) Boolean statisticSamplingEnabled, 
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      specifiedDefaultValue="false",
      help = CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ__HELP) Boolean setCopyOnRead,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE__HELP) Integer lockLease,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT__HELP) Integer lockTimeout,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL__HELP) Integer messageSyncInterval,
      @CliOption (key = {CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT},
      unspecifiedDefaultValue=CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT__HELP) Integer searchTimeout
      ) {

    Map<String, String> runTimeDistributionConfigAttributes = new HashMap<String, String>();
    Map<String, String> rumTimeCacheAttributes = new HashMap<String, String>();
    Set<DistributedMember> targetMembers = new HashSet<DistributedMember>();
    
    try {
      
      targetMembers = CliUtil.findAllMatchingMembers(group, memberNameOrId);

      if (archiveDiskSpaceLimit != null) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, archiveDiskSpaceLimit.toString());
      }

      if (archiveFileSizeLimit != null) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, archiveFileSizeLimit.toString());
      }

      if (logDiskSpaceLimit != null) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, logDiskSpaceLimit.toString());
      }

      if (logFileSizeLimit != null) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, logFileSizeLimit.toString());
      }

      if (logLevel != null && !logLevel.isEmpty()) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
      }

      if (statisticArchiveFile != null && !statisticArchiveFile.isEmpty()) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statisticArchiveFile);
      }

      if (statisticSampleRate != null) {
        runTimeDistributionConfigAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, statisticSampleRate.toString());
      }

      if (statisticSamplingEnabled != null) {
        runTimeDistributionConfigAttributes.put(STATISTIC_SAMPLING_ENABLED, statisticSamplingEnabled.toString());
      }
      
      
      //Attributes that are set on the cache.
      if (setCopyOnRead != null) {
        rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ, setCopyOnRead.toString());
      }
      
      if (lockLease != null && lockLease > 0 && lockLease < Integer.MAX_VALUE) {
        rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE, lockLease.toString());
      }
      
      if (lockTimeout != null && lockTimeout > 0 && lockTimeout < Integer.MAX_VALUE) {
        rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT,  lockTimeout.toString());
      }
      
      if (messageSyncInterval != null && messageSyncInterval > 0 && messageSyncInterval < Integer.MAX_VALUE) {
        rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL,  messageSyncInterval.toString());
      }
      
      if (searchTimeout != null && searchTimeout > 0 && searchTimeout < Integer.MAX_VALUE) {
        rumTimeCacheAttributes.put(CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT,  searchTimeout.toString());
      }
      
      if (!runTimeDistributionConfigAttributes.isEmpty() || !rumTimeCacheAttributes.isEmpty()) {
        Map<String, String> allRunTimeAttributes = new HashMap<String, String>();
        allRunTimeAttributes.putAll(runTimeDistributionConfigAttributes);
        allRunTimeAttributes.putAll(rumTimeCacheAttributes);
        
        ResultCollector<?,?> rc = CliUtil.executeFunction(alterRunTimeConfigFunction, allRunTimeAttributes, targetMembers);
        List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
        CompositeResultData crd = ResultBuilder.createCompositeResultData();
        TabularResultData tabularData = crd.addSection().addTable();
        Set<String> successfulMembers = new TreeSet<String>();
        Set<String> errorMessages = new TreeSet<String>();


        for (CliFunctionResult result : results) {
          if (result.getThrowable() != null) {
            errorMessages.add(result.getThrowable().getMessage());
          } else {
            successfulMembers.add(result.getMemberIdOrName());
          }
        }
        final String lineSeparator = System.getProperty("line.separator");
        if (!successfulMembers.isEmpty()) {
          StringBuilder successMessageBuilder = new StringBuilder();

          successMessageBuilder.append(CliStrings.ALTER_RUNTIME_CONFIG__SUCCESS__MESSAGE);
          successMessageBuilder.append(lineSeparator);

          for (String member : successfulMembers) {
            successMessageBuilder.append(member);
            successMessageBuilder.append(lineSeparator);
          }
          
          Properties properties = new Properties();
          properties.putAll(runTimeDistributionConfigAttributes);
          
          Result result = ResultBuilder.createInfoResult(successMessageBuilder.toString());
          
          //Set the Cache attributes to be modified
          final XmlEntity xmlEntity = XmlEntity.builder().withType(CacheXml.CACHE).withAttributes(rumTimeCacheAttributes).build();
          result.setCommandPersisted(new SharedConfigurationWriter().modifyPropertiesAndCacheAttributes(properties, xmlEntity, group !=null ? group.split(",") : null));
          return result;
        } else {
          StringBuilder errorMessageBuilder = new StringBuilder();
          errorMessageBuilder.append("Following errors occurred while altering runtime config");
          errorMessageBuilder.append(lineSeparator);

          for (String errorMessage : errorMessages){
            errorMessageBuilder.append(errorMessage);
            errorMessageBuilder.append(lineSeparator);
          }
          return ResultBuilder.createUserErrorResult(errorMessageBuilder.toString());
        }
      } else {
        return ResultBuilder.createUserErrorResult(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);
      }
    } catch (CommandResultException crex) {
      return crex.getResult();
    }catch (CacheClosedException e) {
      return ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.ALTER_RUNTIME_CONFIG));
    } catch (Exception e) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE, e.getClass(), e.getMessage()));
    }
  }

  @CliAvailabilityIndicator({ CliStrings.DESCRIBE_CONFIG, CliStrings.EXPORT_CONFIG, CliStrings.ALTER_RUNTIME_CONFIG})
  public boolean configCommandsAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) { // in gfsh check if connected
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }

  /**
   * Interceptor used by gfsh to intercept execution of export config command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private String saveDirString;

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();
      String dir = paramValueMap.get("dir");
      dir = (dir == null) ? null : dir.trim();

      File saveDirFile = new File(".");
      if (dir != null && !dir.isEmpty()) {
        saveDirFile = new File(dir);
        if (saveDirFile.exists()) {
          if (!saveDirFile.isDirectory())
            return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_A_DIRECTORY, dir));
        } else if (!saveDirFile.mkdirs()) {
          return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__CANNOT_CREATE_DIR, dir));
        }
      }
      try {
        if (!saveDirFile.canWrite()) {
          return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile
              .getCanonicalPath()));
        }
      } catch (IOException ioex) {
        return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile
            .getName()));
      }

      saveDirString = saveDirFile.getAbsolutePath();
      return ResultBuilder.createInfoResult("OK");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      if (commandResult.hasIncomingFiles()) {
        try {
          commandResult.saveIncomingFiles(saveDirString);
        } catch (IOException ioex) {
          getGfsh().logSevere("Unable to export config", ioex);
        }
      }

      return commandResult;
    }
  }
}
