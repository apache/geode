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
package com.gemstone.gemfire.cache.lucene.internal.cli;

import static com.gemstone.gemfire.cache.operations.OperationContext.*;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.geode.security.GeodePermission.Operation;
import org.apache.geode.security.GeodePermission.Resource;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;

/**
 * The LuceneIndexCommands class encapsulates all Geode shell (Gfsh) commands related to Lucene indexes defined in Geode.
 * </p>
 * @see AbstractCommandsSupport
 * @see LuceneIndexDetails
 * @see LuceneListIndexFunction
 */
@SuppressWarnings("unused")
public class LuceneIndexCommands extends AbstractCommandsSupport {
  private static final LuceneCreateIndexFunction createIndexFunction = new LuceneCreateIndexFunction();
  private static final LuceneDescribeIndexFunction describeIndexFunction = new LuceneDescribeIndexFunction();

  @CliCommand(value = LuceneCliStrings.LUCENE_LIST_INDEX, help = LuceneCliStrings.LUCENE_LIST_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic={CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listIndex(
    @CliOption(key = LuceneCliStrings.LUCENE_LIST_INDEX__STATS,
      mandatory=false,
      specifiedDefaultValue = "true",
      unspecifiedDefaultValue = "false",
      help = LuceneCliStrings.LUCENE_LIST_INDEX__STATS__HELP) final boolean stats) {

    try {
      return toTabularResult(getIndexListing(),stats);
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        LuceneCliStrings.LUCENE_LIST_INDEX));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().error(t);
      return ResultBuilder.createGemFireErrorResult(String.format(LuceneCliStrings.LUCENE_LIST_INDEX__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexListing() {
    final Execution functionExecutor = getMembersFunctionExecutor(getMembers(getCache()));

    if (functionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) functionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector resultsCollector = functionExecutor.execute(new LuceneListIndexFunction());
    final List<Set<LuceneIndexDetails>> results = (List<Set<LuceneIndexDetails>>) resultsCollector.getResult();

    return results.stream()
      .flatMap(set -> set.stream())
      .sorted()
      .collect(Collectors.toList());
  }

  protected Result toTabularResult(final List<LuceneIndexDetails> indexDetailsList, boolean stats) {
    if (!indexDetailsList.isEmpty()) {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();

      for (final LuceneIndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Index Name", indexDetails.getIndexName());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Indexed Fields", indexDetails.getSearchableFieldNamesString());
        indexData.accumulate("Field Analyzer", indexDetails.getFieldAnalyzersString());

        if (stats==true) {
          indexData.accumulate("Query Executions",indexDetails.getIndexStats().get("queryExecutions"));
          indexData.accumulate("Updates",indexDetails.getIndexStats().get("updates"));
          indexData.accumulate("Commits",indexDetails.getIndexStats().get("commits"));
          indexData.accumulate("Documents",indexDetails.getIndexStats().get("documents"));
        }
      }
      return ResultBuilder.buildResult(indexData);
    }
    else {
      return ResultBuilder.createInfoResult(LuceneCliStrings.LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

  @CliCommand(value = LuceneCliStrings.LUCENE_CREATE_INDEX, help = LuceneCliStrings.LUCENE_CREATE_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic={CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA }, writesToSharedConfiguration=true)
  //TODO : Add optionContext for indexName
  public Result createIndex(
    @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME,
      mandatory=true,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__NAME__HELP) final String indexName,

    @CliOption (key = LuceneCliStrings.LUCENE__REGION_PATH,
      mandatory = true,
      optionContext = ConverterHint.REGIONPATH,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__REGION_HELP) final String regionPath,

    @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,
      mandatory = true,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD_HELP)
    @CliMetaData (valueSeparator = ",") final String[] fields,

    @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
      mandatory = false,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER_HELP)
    @CliMetaData (valueSeparator = ",") final String[] analyzers,

    @CliOption (key = LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP,
      optionContext = ConverterHint.MEMBERGROUP,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP__HELP)
    @CliMetaData (valueSeparator = ",") final String[] groups) {

    Result result = null;
    XmlEntity xmlEntity = null;

    GeodeSecurityUtil.authorizeRegionManage(regionPath);
    try {
      final Cache cache = getCache();
      LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath, fields, analyzers);
      final ResultCollector<?, ?> rc = this.executeFunctionOnGroups(createIndexFunction, groups, indexInfo);
      final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();

      final Set<String> successfulMembers = new TreeSet<String>();
      final Map<String, Set<String>> indexOpFailMap = new HashMap<String, Set<String>>();

      for (final CliFunctionResult cliFunctionResult : funcResults) {

          if (cliFunctionResult.isSuccessful()) {
            successfulMembers.add(cliFunctionResult.getMemberIdOrName());

            if (xmlEntity == null) {
              xmlEntity = cliFunctionResult.getXmlEntity();
            }
          }
          else {
            final String exceptionMessage = cliFunctionResult.getMessage();
            Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

            if (failedMembers == null) {
              failedMembers = new TreeSet<String>();
            }
            failedMembers.add(cliFunctionResult.getMemberIdOrName());
            indexOpFailMap.put(exceptionMessage, failedMembers);
          }
      }

      if (!successfulMembers.isEmpty()) {

        final InfoResultData infoResult = ResultBuilder.createInfoResultData();
        infoResult.addLine(LuceneCliStrings.CREATE_INDEX__SUCCESS__MSG);
        infoResult.addLine(CliStrings.format(LuceneCliStrings.CREATE_INDEX__NAME__MSG, indexName));
        infoResult.addLine(CliStrings.format(LuceneCliStrings.CREATE_INDEX__REGIONPATH__MSG, regionPath));
        infoResult.addLine(LuceneCliStrings.CREATE_INDEX__MEMBER__MSG);

        int num = 0;

        for (final String memberId : successfulMembers) {
          ++num;
          infoResult.addLine(CliStrings.format(LuceneCliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
        }
        result = ResultBuilder.buildResult(infoResult);


      } else {
        //Group members by the exception thrown.
        final ErrorResultData erd = ResultBuilder.createErrorResultData();
        erd.addLine(CliStrings.format(LuceneCliStrings.CREATE_INDEX__FAILURE__MSG, indexName));

        final Set<String> exceptionMessages = indexOpFailMap.keySet();

        for (final String exceptionMessage : exceptionMessages) {
          erd.addLine(exceptionMessage);
          erd.addLine(LuceneCliStrings.CREATE_INDEX__EXCEPTION__OCCURRED__ON);
          final Set<String> memberIds = indexOpFailMap.get(exceptionMessage);

          int num = 0;
          for (final String memberId : memberIds) {
            ++num;
            erd.addLine(CliStrings.format(LuceneCliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
          }
        }
        result = ResultBuilder.buildResult(erd);
      }
    } catch (CommandResultException crex) {
      result = crex.getResult();
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
//    TODO - store in cluster config
//    if (xmlEntity != null) {
//      result.setCommandPersisted((new SharedConfigurationWriter()).addXmlEntity(xmlEntity, groups));
//    }

    return result;
  }

  @CliCommand(value = LuceneCliStrings.LUCENE_DESCRIBE_INDEX, help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic={CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result describeIndex(
    @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME,
      mandatory=true,
      help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__NAME__HELP) final String indexName,

    @CliOption (key = LuceneCliStrings.LUCENE__REGION_PATH,
      mandatory = true,
      optionContext = ConverterHint.REGIONPATH,
      help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__REGION_HELP) final String regionPath) {
    try {
      LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath);
      return toTabularResult(getIndexDetails(indexInfo),true);
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        LuceneCliStrings.LUCENE_DESCRIBE_INDEX));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().error(t);
      return ResultBuilder.createGemFireErrorResult(String.format(LuceneCliStrings.LUCENE_DESCRIBE_INDEX__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexDetails(LuceneIndexInfo indexInfo) throws Exception {
    GeodeSecurityUtil.authorizeRegionManage(indexInfo.getRegionPath());
    final String[] groups = {};
    final ResultCollector<?, ?> rc = this.executeFunctionOnGroups(describeIndexFunction, groups, indexInfo);
    final List<LuceneIndexDetails> funcResults = (List<LuceneIndexDetails>) rc.getResult();
    return funcResults.stream().filter(indexDetails -> indexDetails != null).collect(Collectors.toList());
  }

  protected ResultCollector<?, ?> executeFunctionOnGroups(FunctionAdapter function, String[]groups, final LuceneIndexInfo indexInfo) throws CommandResultException {
    final Set<DistributedMember> targetMembers = CliUtil.findAllMatchingMembers(groups, null);
    return CliUtil.executeFunction(function, indexInfo, targetMembers);
  }
}