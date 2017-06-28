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
package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.IndexDetails;
import org.apache.geode.management.internal.cli.domain.IndexDetails.IndexStatisticsDetails;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDefinedIndexesFunction;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.functions.DestroyIndexFunction;
import org.apache.geode.management.internal.cli.functions.ListIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * The IndexCommands class encapsulates all GemFire shell (Gfsh) commands related to indexes defined
 * in GemFire.
 *
 * @see GfshCommand
 * @see org.apache.geode.management.internal.cli.domain.IndexDetails
 * @see org.apache.geode.management.internal.cli.functions.ListIndexFunction
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class IndexCommands implements GfshCommand {

  private static final CreateIndexFunction createIndexFunction = new CreateIndexFunction();
  private static final DestroyIndexFunction destroyIndexFunction = new DestroyIndexFunction();
  private static final CreateDefinedIndexesFunction createDefinedIndexesFunction =
      new CreateDefinedIndexesFunction();
  private static final Set<IndexInfo> indexDefinitions =
      Collections.synchronizedSet(new HashSet<IndexInfo>());

  @CliCommand(value = CliStrings.LIST_INDEX, help = CliStrings.LIST_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ, target = Target.QUERY)
  public Result listIndex(@CliOption(key = CliStrings.LIST_INDEX__STATS,
      specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
      help = CliStrings.LIST_INDEX__STATS__HELP) final boolean showStats) {
    try {
      return toTabularResult(getIndexListing(), showStats);
    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.LIST_INDEX));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().error(t);
      return ResultBuilder.createGemFireErrorResult(
          String.format(CliStrings.LIST_INDEX__ERROR_MESSAGE, toString(t, isDebugging())));
    }
  }

  @SuppressWarnings("unchecked")
  protected List<IndexDetails> getIndexListing() {
    final Execution functionExecutor = getMembersFunctionExecutor(getMembers(getCache()));

    if (functionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) functionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultsCollector =
        functionExecutor.execute(new ListIndexFunction());

    final List<?> results = (List<?>) resultsCollector.getResult();
    final List<IndexDetails> indexDetailsList = new ArrayList<>(results.size());

    for (Object result : results) {
      if (result instanceof Set) { // ignore FunctionInvocationTargetExceptions and other Exceptions
        indexDetailsList.addAll((Set<IndexDetails>) result);
      }
    }

    Collections.sort(indexDetailsList);

    return indexDetailsList;
  }

  protected Result toTabularResult(final List<IndexDetails> indexDetailsList,
      final boolean showStats) {
    if (!indexDetailsList.isEmpty()) {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();

      for (final IndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Member Name",
            StringUtils.defaultString(indexDetails.getMemberName()));
        indexData.accumulate("Member ID", indexDetails.getMemberId());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Name", indexDetails.getIndexName());
        indexData.accumulate("Type", StringUtils.defaultString(indexDetails.getIndexType()));
        indexData.accumulate("Indexed Expression", indexDetails.getIndexedExpression());
        indexData.accumulate("From Clause", indexDetails.getFromClause());

        if (showStats) {
          final IndexStatisticsDetailsAdapter adapter =
              new IndexStatisticsDetailsAdapter(indexDetails.getIndexStatisticsDetails());

          indexData.accumulate("Uses", adapter.getTotalUses());
          indexData.accumulate("Updates", adapter.getNumberOfUpdates());
          indexData.accumulate("Update Time", adapter.getTotalUpdateTime());
          indexData.accumulate("Keys", adapter.getNumberOfKeys());
          indexData.accumulate("Values", adapter.getNumberOfValues());
        }
      }

      return ResultBuilder.buildResult(indexData);
    } else {
      return ResultBuilder.createInfoResult(CliStrings.LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

  @CliCommand(value = CliStrings.CREATE_INDEX, help = CliStrings.CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  public Result createIndex(@CliOption(key = CliStrings.CREATE_INDEX__NAME, mandatory = true,
      help = CliStrings.CREATE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = CliStrings.CREATE_INDEX__EXPRESSION, mandatory = true,
          help = CliStrings.CREATE_INDEX__EXPRESSION__HELP) final String indexedExpression,

      @CliOption(key = CliStrings.CREATE_INDEX__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_INDEX__REGION__HELP) String regionPath,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_INDEX__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = CliStrings.CREATE_INDEX__TYPE, unspecifiedDefaultValue = "range",
          optionContext = ConverterHint.INDEX_TYPE,
          help = CliStrings.CREATE_INDEX__TYPE__HELP) final String indexType,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_INDEX__GROUP__HELP) final String[] group) {

    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    try {
      final Cache cache = CacheFactory.getAnyInstance();

      int idxType;

      // Index type check
      if ("range".equalsIgnoreCase(indexType)) {
        idxType = IndexInfo.RANGE_INDEX;
      } else if ("hash".equalsIgnoreCase(indexType)) {
        idxType = IndexInfo.HASH_INDEX;
      } else if ("key".equalsIgnoreCase(indexType)) {
        idxType = IndexInfo.KEY_INDEX;
      } else {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CREATE_INDEX__INVALID__INDEX__TYPE__MESSAGE);
      }

      if (indexName == null || indexName.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.CREATE_INDEX__INVALID__INDEX__NAME);
      }

      if (indexedExpression == null || indexedExpression.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.CREATE_INDEX__INVALID__EXPRESSION);
      }

      if (StringUtils.isBlank(regionPath) || regionPath.equals(Region.SEPARATOR)) {
        return ResultBuilder.createUserErrorResult(CliStrings.CREATE_INDEX__INVALID__REGIONPATH);
      }

      if (!regionPath.startsWith(Region.SEPARATOR)) {
        regionPath = Region.SEPARATOR + regionPath;
      }

      IndexInfo indexInfo = new IndexInfo(indexName, indexedExpression, regionPath, idxType);

      final Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrID);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(createIndexFunction, indexInfo, targetMembers);

      final List<Object> funcResults = (List<Object>) rc.getResult();
      final Set<String> successfulMembers = new TreeSet<>();
      final Map<String, Set<String>> indexOpFailMap = new HashMap<>();

      for (final Object funcResult : funcResults) {
        if (funcResult instanceof CliFunctionResult) {
          final CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

          if (cliFunctionResult.isSuccessful()) {
            successfulMembers.add(cliFunctionResult.getMemberIdOrName());

            if (xmlEntity.get() == null) {
              xmlEntity.set(cliFunctionResult.getXmlEntity());
            }
          } else {
            final String exceptionMessage = cliFunctionResult.getMessage();
            Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

            if (failedMembers == null) {
              failedMembers = new TreeSet<>();
            }
            failedMembers.add(cliFunctionResult.getMemberIdOrName());
            indexOpFailMap.put(exceptionMessage, failedMembers);
          }
        }
      }

      if (!successfulMembers.isEmpty()) {

        final InfoResultData infoResult = ResultBuilder.createInfoResultData();
        infoResult.addLine(CliStrings.CREATE_INDEX__SUCCESS__MSG);
        infoResult.addLine(CliStrings.format(CliStrings.CREATE_INDEX__NAME__MSG, indexName));
        infoResult.addLine(
            CliStrings.format(CliStrings.CREATE_INDEX__EXPRESSION__MSG, indexedExpression));
        infoResult.addLine(CliStrings.format(CliStrings.CREATE_INDEX__REGIONPATH__MSG, regionPath));
        infoResult.addLine(CliStrings.CREATE_INDEX__MEMBER__MSG);

        int num = 0;

        for (final String memberId : successfulMembers) {
          ++num;
          infoResult.addLine(
              CliStrings.format(CliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
        }
        result = ResultBuilder.buildResult(infoResult);

      } else {
        // Group members by the exception thrown.
        final ErrorResultData erd = ResultBuilder.createErrorResultData();
        erd.addLine(CliStrings.format(CliStrings.CREATE_INDEX__FAILURE__MSG, indexName));

        final Set<String> exceptionMessages = indexOpFailMap.keySet();

        for (final String exceptionMessage : exceptionMessages) {
          erd.addLine(exceptionMessage);
          erd.addLine(CliStrings.CREATE_INDEX__EXCEPTION__OCCURRED__ON);
          final Set<String> memberIds = indexOpFailMap.get(exceptionMessage);

          int num = 0;
          for (final String memberId : memberIds) {
            ++num;
            erd.addLine(
                CliStrings.format(CliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
          }
        }
        result = ResultBuilder.buildResult(erd);
      }
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), group));
    }

    return result;
  }

  @CliCommand(value = CliStrings.DESTROY_INDEX, help = CliStrings.DESTROY_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  public Result destroyIndex(
      @CliOption(key = CliStrings.DESTROY_INDEX__NAME, unspecifiedDefaultValue = "",
          help = CliStrings.DESTROY_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = CliStrings.DESTROY_INDEX__REGION, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DESTROY_INDEX__REGION__HELP) final String regionPath,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.DESTROY_INDEX__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.DESTROY_INDEX__GROUP__HELP) final String[] group) {

    Result result;

    if (StringUtils.isBlank(indexName) && StringUtils.isBlank(regionPath)
        && ArrayUtils.isEmpty(group) && ArrayUtils.isEmpty(memberNameOrID)) {
      return ResultBuilder.createUserErrorResult(
          CliStrings.format(CliStrings.PROVIDE_ATLEAST_ONE_OPTION, CliStrings.DESTROY_INDEX));
    }

    final Cache cache = CacheFactory.getAnyInstance();
    String regionName = null;
    if (regionPath != null) {
      regionName = regionPath.startsWith("/") ? regionPath.substring(1) : regionPath;
    }
    IndexInfo indexInfo = new IndexInfo(indexName, regionName);
    Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultCollector rc = CliUtil.executeFunction(destroyIndexFunction, indexInfo, targetMembers);
    List<Object> funcResults = (List<Object>) rc.getResult();

    Set<String> successfulMembers = new TreeSet<>();
    Map<String, Set<String>> indexOpFailMap = new HashMap<>();

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    for (Object funcResult : funcResults) {
      if (!(funcResult instanceof CliFunctionResult)) {
        continue;
      }

      CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

      if (cliFunctionResult.isSuccessful()) {
        successfulMembers.add(cliFunctionResult.getMemberIdOrName());
        if (xmlEntity.get() == null) {
          xmlEntity.set(cliFunctionResult.getXmlEntity());
        }
      } else {
        String exceptionMessage = cliFunctionResult.getMessage();
        Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

        if (failedMembers == null) {
          failedMembers = new TreeSet<>();
        }
        failedMembers.add(cliFunctionResult.getMemberIdOrName());
        indexOpFailMap.put(exceptionMessage, failedMembers);
      }
    }

    if (!successfulMembers.isEmpty()) {
      InfoResultData infoResult = ResultBuilder.createInfoResultData();

      if (StringUtils.isNotBlank(indexName)) {
        if (StringUtils.isNotBlank(regionPath)) {
          infoResult.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__ON__REGION__SUCCESS__MSG,
              indexName, regionPath));
        } else {
          infoResult.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__SUCCESS__MSG, indexName));
        }
      } else {
        if (StringUtils.isNotBlank(regionPath)) {
          infoResult.addLine(CliStrings
              .format(CliStrings.DESTROY_INDEX__ON__REGION__ONLY__SUCCESS__MSG, regionPath));
        } else {
          infoResult.addLine(CliStrings.DESTROY_INDEX__ON__MEMBERS__ONLY__SUCCESS__MSG);
        }
      }

      int num = 0;
      for (String memberId : successfulMembers) {
        infoResult.addLine(CliStrings.format(
            CliStrings.format(CliStrings.DESTROY_INDEX__NUMBER__AND__MEMBER, ++num, memberId)));
      }
      result = ResultBuilder.buildResult(infoResult);

    } else {

      ErrorResultData erd = ResultBuilder.createErrorResultData();
      if (StringUtils.isNotBlank(indexName)) {
        erd.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__FAILURE__MSG, indexName));
      } else {
        erd.addLine("Indexes could not be destroyed for following reasons");
      }

      Set<String> exceptionMessages = indexOpFailMap.keySet();

      for (String exceptionMessage : exceptionMessages) {
        erd.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__REASON_MESSAGE, exceptionMessage));
        erd.addLine(CliStrings.DESTROY_INDEX__EXCEPTION__OCCURRED__ON);

        Set<String> memberIds = indexOpFailMap.get(exceptionMessage);
        int num = 0;

        for (String memberId : memberIds) {
          erd.addLine(CliStrings.format(
              CliStrings.format(CliStrings.DESTROY_INDEX__NUMBER__AND__MEMBER, ++num, memberId)));
        }
        erd.addLine("");
      }
      result = ResultBuilder.buildResult(erd);
    }
    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), group));
    }

    return result;
  }

  @CliCommand(value = CliStrings.DEFINE_INDEX, help = CliStrings.DEFINE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  public Result defineIndex(@CliOption(key = CliStrings.DEFINE_INDEX_NAME, mandatory = true,
      help = CliStrings.DEFINE_INDEX__HELP) final String indexName,

      @CliOption(key = CliStrings.DEFINE_INDEX__EXPRESSION, mandatory = true,
          help = CliStrings.DEFINE_INDEX__EXPRESSION__HELP) final String indexedExpression,

      @CliOption(key = CliStrings.DEFINE_INDEX__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DEFINE_INDEX__REGION__HELP) String regionPath,

      @CliOption(key = CliStrings.DEFINE_INDEX__TYPE, unspecifiedDefaultValue = "range",
          optionContext = ConverterHint.INDEX_TYPE,
          help = CliStrings.DEFINE_INDEX__TYPE__HELP) final String indexType) {

    Result result;
    XmlEntity xmlEntity = null;

    int idxType;

    // Index type check
    if ("range".equalsIgnoreCase(indexType)) {
      idxType = IndexInfo.RANGE_INDEX;
    } else if ("hash".equalsIgnoreCase(indexType)) {
      idxType = IndexInfo.HASH_INDEX;
    } else if ("key".equalsIgnoreCase(indexType)) {
      idxType = IndexInfo.KEY_INDEX;
    } else {
      return ResultBuilder
          .createUserErrorResult(CliStrings.DEFINE_INDEX__INVALID__INDEX__TYPE__MESSAGE);
    }

    if (indexName == null || indexName.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.DEFINE_INDEX__INVALID__INDEX__NAME);
    }

    if (indexedExpression == null || indexedExpression.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.DEFINE_INDEX__INVALID__EXPRESSION);
    }

    if (StringUtils.isBlank(regionPath) || regionPath.equals(Region.SEPARATOR)) {
      return ResultBuilder.createUserErrorResult(CliStrings.DEFINE_INDEX__INVALID__REGIONPATH);
    }

    if (!regionPath.startsWith(Region.SEPARATOR)) {
      regionPath = Region.SEPARATOR + regionPath;
    }

    IndexInfo indexInfo = new IndexInfo(indexName, indexedExpression, regionPath, idxType);
    indexDefinitions.add(indexInfo);

    final InfoResultData infoResult = ResultBuilder.createInfoResultData();
    infoResult.addLine(CliStrings.DEFINE_INDEX__SUCCESS__MSG);
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__NAME__MSG, indexName));
    infoResult
        .addLine(CliStrings.format(CliStrings.DEFINE_INDEX__EXPRESSION__MSG, indexedExpression));
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__REGIONPATH__MSG, regionPath));
    result = ResultBuilder.buildResult(infoResult);

    return result;
  }

  @CliCommand(value = CliStrings.CREATE_DEFINED_INDEXES, help = CliStrings.CREATE_DEFINED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  // TODO : Add optionContext for indexName
  public Result createDefinedIndexes(

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_DEFINED_INDEXES__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_DEFINED_INDEXES__GROUP__HELP) final String[] group) {

    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    if (indexDefinitions.isEmpty()) {
      final InfoResultData infoResult = ResultBuilder.createInfoResultData();
      infoResult.addLine(CliStrings.DEFINE_INDEX__FAILURE__MSG);
      return ResultBuilder.buildResult(infoResult);
    }

    try {
      final Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrID);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final Cache cache = CacheFactory.getAnyInstance();
      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(createDefinedIndexesFunction, indexDefinitions, targetMembers);

      final List<Object> funcResults = (List<Object>) rc.getResult();
      final Set<String> successfulMembers = new TreeSet<>();
      final Map<String, Set<String>> indexOpFailMap = new HashMap<>();

      for (final Object funcResult : funcResults) {
        if (funcResult instanceof CliFunctionResult) {
          final CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

          if (cliFunctionResult.isSuccessful()) {
            successfulMembers.add(cliFunctionResult.getMemberIdOrName());

            if (xmlEntity.get() == null) {
              xmlEntity.set(cliFunctionResult.getXmlEntity());
            }
          } else {
            final String exceptionMessage = cliFunctionResult.getMessage();
            Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

            if (failedMembers == null) {
              failedMembers = new TreeSet<>();
            }
            failedMembers.add(cliFunctionResult.getMemberIdOrName());
            indexOpFailMap.put(exceptionMessage, failedMembers);
          }
        }
      }

      if (!successfulMembers.isEmpty()) {
        final InfoResultData infoResult = ResultBuilder.createInfoResultData();
        infoResult.addLine(CliStrings.CREATE_DEFINED_INDEXES__SUCCESS__MSG);

        int num = 0;

        for (final String memberId : successfulMembers) {
          ++num;
          infoResult.addLine(CliStrings
              .format(CliStrings.CREATE_DEFINED_INDEXES__NUMBER__AND__MEMBER, num, memberId));
        }
        result = ResultBuilder.buildResult(infoResult);

      } else {
        // Group members by the exception thrown.
        final ErrorResultData erd = ResultBuilder.createErrorResultData();

        final Set<String> exceptionMessages = indexOpFailMap.keySet();

        for (final String exceptionMessage : exceptionMessages) {
          erd.addLine(exceptionMessage);
          erd.addLine(CliStrings.CREATE_INDEX__EXCEPTION__OCCURRED__ON);
          final Set<String> memberIds = indexOpFailMap.get(exceptionMessage);

          int num = 0;
          for (final String memberId : memberIds) {
            ++num;
            erd.addLine(CliStrings.format(CliStrings.CREATE_DEFINED_INDEXES__NUMBER__AND__MEMBER,
                num, memberId));
          }
        }
        result = ResultBuilder.buildResult(erd);
      }
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), group));
    }
    return result;
  }

  @CliCommand(value = CliStrings.CLEAR_DEFINED_INDEXES, help = CliStrings.CLEAR_DEFINED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  // TODO : Add optionContext for indexName
  public Result clearDefinedIndexes() {
    indexDefinitions.clear();
    final InfoResultData infoResult = ResultBuilder.createInfoResultData();
    infoResult.addLine(CliStrings.CLEAR_DEFINED_INDEX__SUCCESS__MSG);
    return ResultBuilder.buildResult(infoResult);
  }

  protected static class IndexStatisticsDetailsAdapter {

    private final IndexStatisticsDetails indexStatisticsDetails;

    protected IndexStatisticsDetailsAdapter(final IndexStatisticsDetails indexStatisticsDetails) {
      this.indexStatisticsDetails = indexStatisticsDetails;
    }

    public IndexStatisticsDetails getIndexStatisticsDetails() {
      return indexStatisticsDetails;
    }

    public String getNumberOfKeys() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfKeys()) : "";
    }

    public String getNumberOfUpdates() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfUpdates()) : "";
    }

    public String getNumberOfValues() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfValues()) : "";
    }

    public String getTotalUpdateTime() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUpdateTime()) : "";
    }

    public String getTotalUses() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUses()) : "";
    }
  }

}
