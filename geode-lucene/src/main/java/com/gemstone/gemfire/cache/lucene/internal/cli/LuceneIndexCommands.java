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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
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
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneSearchIndexFunction;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.security.IntegratedSecurityService;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
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
  private static final LuceneSearchIndexFunction searchIndexFunction = new LuceneSearchIndexFunction();
  private List<LuceneSearchResults> searchResults=null;

  private SecurityService securityService = IntegratedSecurityService.getSecurityService();

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
      getCache().getLogger().info(t);
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
        indexData.accumulate("Status", indexDetails.getInitialized() == true ? "Initialized" : "Defined");

        if (stats == true) {
          if (!indexDetails.getInitialized()) {
            indexData.accumulate("Query Executions", "NA");
            indexData.accumulate("Updates", "NA");
            indexData.accumulate("Commits", "NA");
            indexData.accumulate("Documents", "NA");
          }
          else {
            indexData.accumulate("Query Executions", indexDetails.getIndexStats().get("queryExecutions"));
            indexData.accumulate("Updates", indexDetails.getIndexStats().get("updates"));
            indexData.accumulate("Commits", indexDetails.getIndexStats().get("commits"));
            indexData.accumulate("Documents", indexDetails.getIndexStats().get("documents"));
          }
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

    this.securityService.authorizeRegionManage(regionPath);
    try {
      final Cache cache = getCache();
      LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath, fields, analyzers);
      final ResultCollector<?, ?> rc = this.executeFunctionOnGroups(createIndexFunction, groups, indexInfo);
      final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();

      final TabularResultData tabularResult = ResultBuilder.createTabularResultData();
      for (final CliFunctionResult cliFunctionResult : funcResults) {
        tabularResult.accumulate("Member",cliFunctionResult.getMemberIdOrName());

          if (cliFunctionResult.isSuccessful()) {
            tabularResult.accumulate("Status","Successfully created lucene index");
//            if (xmlEntity == null) {
//              xmlEntity = cliFunctionResult.getXmlEntity();
//            }
          }
          else {
            tabularResult.accumulate("Status","Failed: "+cliFunctionResult.getMessage());
          }
      }
        result = ResultBuilder.buildResult(tabularResult);
      }
     catch (CommandResultException crex) {
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
    catch (IllegalArgumentException e) {
      return ResultBuilder.createInfoResult(e.getMessage());
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().info(t);
      return ResultBuilder.createGemFireErrorResult(String.format(LuceneCliStrings.LUCENE_DESCRIBE_INDEX__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexDetails(LuceneIndexInfo indexInfo) throws Exception {
    this.securityService.authorizeRegionManage(indexInfo.getRegionPath());
    final ResultCollector<?, ?> rc = this.executeFunctionOnGroups(describeIndexFunction, new String[] {}, indexInfo);
    final List<LuceneIndexDetails> funcResults = (List<LuceneIndexDetails>) rc.getResult();
    return funcResults.stream().filter(indexDetails -> indexDetails != null).collect(Collectors.toList());
  }

  @CliCommand(value = LuceneCliStrings.LUCENE_SEARCH_INDEX, help = LuceneCliStrings.LUCENE_SEARCH_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = { CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result searchIndex(
    @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME,
      mandatory = true,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__NAME__HELP) final String indexName,

    @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH,
      mandatory = true,
      optionContext = ConverterHint.REGIONPATH,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__REGION_HELP) final String regionPath,

    @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,
      mandatory = true,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING__HELP) final String queryString,

    @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,
      mandatory = true,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD__HELP) final String defaultField,

    @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT,
      mandatory = false,
      unspecifiedDefaultValue = "-1",
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT__HELP) final int limit,

    @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__PAGE_SIZE,
      mandatory = false,
      unspecifiedDefaultValue = "-1",
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__PAGE_SIZE__HELP) int pageSize,

    @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY,
      mandatory = false,
      unspecifiedDefaultValue = "false",
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY__HELP) boolean keysOnly)
  {
    try {
      LuceneQueryInfo queryInfo = new LuceneQueryInfo(indexName, regionPath, queryString, defaultField, limit, keysOnly);
      if (pageSize == -1) {
        pageSize = Integer.MAX_VALUE;
      }
      searchResults = getSearchResults(queryInfo);
      return displayResults(pageSize, keysOnly);
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        LuceneCliStrings.LUCENE_SEARCH_INDEX));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (IllegalArgumentException e) {
      return ResultBuilder.createInfoResult(e.getMessage());
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().info(t);
      return ResultBuilder.createGemFireErrorResult(String.format(LuceneCliStrings.LUCENE_SEARCH_INDEX__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

  private Result displayResults(int pageSize, boolean keysOnly) throws Exception {
    if (searchResults.size() == 0) {
      return ResultBuilder.createInfoResult(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE);
    }

    Gfsh gfsh = initGfsh();
    boolean pagination = searchResults.size() > pageSize;
    int fromIndex = 0;
    int toIndex = pageSize < searchResults.size() ? pageSize : searchResults.size();
    int currentPage = 1;
    int totalPages = (int) Math.ceil((float) searchResults.size() / pageSize);
    boolean skipDisplay = false;
    String step = null;
    do {

      if (!skipDisplay) {
        CommandResult commandResult = (CommandResult) getResults(fromIndex, toIndex, keysOnly);
        if (!pagination) {
          return commandResult;
        }
        Gfsh.println();
        while (commandResult.hasNextLine()) {
          gfsh.printAsInfo(commandResult.nextLine());
        }
        gfsh.printAsInfo("\t\tPage " + currentPage + " of " + totalPages);
        String message = ("Press n to move to next page, q to quit and p to previous page : ");
        step = gfsh.interact(message);
      }

      switch (step) {
        case "n":
        {
          if (currentPage == totalPages) {
            gfsh.printAsInfo("No more results to display.");
            step = gfsh.interact("Press p to move to last page and q to quit.");
            skipDisplay = true;
            continue;
          }

          if(skipDisplay) {
            skipDisplay=false;
          }
          else {
            currentPage++;
            int current = fromIndex;
            fromIndex = toIndex;
            toIndex = (pageSize + fromIndex >= searchResults.size()) ? searchResults.size() : pageSize + fromIndex;
          }
          break;
        }
        case "p": {
          if (currentPage == 1) {
            gfsh.printAsInfo("At the top of the search results.");
            step = gfsh.interact("Press n to move to the first page and q to quit.");
            skipDisplay=true;
            continue;
          }

          if (skipDisplay) {
            skipDisplay = false;
          }
          else {
            currentPage--;
            int current = fromIndex;
            toIndex = fromIndex;
            fromIndex = current - pageSize <= 0 ? 0 : current - pageSize;
          }
          break;
        }
        case "q":
          return ResultBuilder.createInfoResult("Search complete.");
        default:
          Gfsh.println("Invalid option");
          break;
      }
    } while(true);
  }

  protected Gfsh initGfsh() {
    return Gfsh.getCurrentInstance();
  }

  private List<LuceneSearchResults> getSearchResults(final LuceneQueryInfo queryInfo) throws Exception {
    securityService.authorizeRegionManage(queryInfo.getRegionPath());

    final String[] groups = {};
    final ResultCollector<?, ?> rc = this.executeSearch(queryInfo);
    final List<Set<LuceneSearchResults>> functionResults = (List<Set<LuceneSearchResults>>) rc.getResult();

    return functionResults.stream()
      .flatMap(set -> set.stream())
      .sorted()
      .collect(Collectors.toList());
  }

  private Result getResults(int fromIndex, int toIndex, boolean keysonly) throws Exception {
    final TabularResultData data = ResultBuilder.createTabularResultData();
    for (int i = fromIndex; i < toIndex; i++) {
      if (!searchResults.get(i).getExeptionFlag()) {
        data.accumulate("key", searchResults.get(i).getKey());
        if (!keysonly) {
          data.accumulate("value", searchResults.get(i).getValue());
          data.accumulate("score", searchResults.get(i).getScore());
        }
      }
      else {
        throw new Exception(searchResults.get(i).getExceptionMessage());
      }
    }
    return ResultBuilder.buildResult(data);
  }

  protected ResultCollector<?, ?> executeFunctionOnGroups(FunctionAdapter function,
                                                          String[] groups,
                                                          final LuceneIndexInfo indexInfo) throws IllegalArgumentException, CommandResultException
  {
    final Set<DistributedMember> targetMembers;
    if (function != createIndexFunction) {
      targetMembers = CliUtil.getMembersForeRegionViaFunction(getCache(), indexInfo.getRegionPath());
      if (targetMembers.isEmpty()) {
        throw new IllegalArgumentException("Region not found.");
      }
    }
    else {
      targetMembers = CliUtil.findAllMatchingMembers(groups, null);
    }
    return CliUtil.executeFunction(function, indexInfo, targetMembers);
  }

  protected ResultCollector<?, ?> executeSearch(final LuceneQueryInfo queryInfo) throws Exception {
    final Set<DistributedMember> targetMembers = CliUtil.getMembersForeRegionViaFunction(getCache(),queryInfo.getRegionPath());
    if (targetMembers.isEmpty())
      throw new IllegalArgumentException("Region not found.");
    return CliUtil.executeFunction(searchIndexFunction, queryInfo, targetMembers);
  }

  @CliAvailabilityIndicator({LuceneCliStrings.LUCENE_SEARCH_INDEX, LuceneCliStrings.LUCENE_CREATE_INDEX,
    LuceneCliStrings.LUCENE_DESCRIBE_INDEX, LuceneCliStrings.LUCENE_LIST_INDEX})
  public boolean indexCommandsAvailable() {
    return (!CliUtil.isGfshVM() || (getGfsh() != null && getGfsh().isConnectedAndReady()));
  }
}
