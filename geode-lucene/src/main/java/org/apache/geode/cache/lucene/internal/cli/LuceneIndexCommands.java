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
package org.apache.geode.cache.lucene.internal.cli;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDestroyIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneSearchIndexFunction;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * The LuceneIndexCommands class encapsulates all Geode shell (Gfsh) commands related to Lucene
 * indexes defined in Geode.
 *
 * @see InternalGfshCommand
 * @see LuceneIndexDetails
 * @see LuceneListIndexFunction
 */
@SuppressWarnings("unused")
public class LuceneIndexCommands extends InternalGfshCommand {
  private static final LuceneCreateIndexFunction createIndexFunction =
      new LuceneCreateIndexFunction();
  private static final LuceneDescribeIndexFunction describeIndexFunction =
      new LuceneDescribeIndexFunction();
  private static final LuceneSearchIndexFunction searchIndexFunction =
      new LuceneSearchIndexFunction();
  private static final LuceneDestroyIndexFunction destroyIndexFunction =
      new LuceneDestroyIndexFunction();
  private List<LuceneSearchResults> searchResults = null;

  @CliCommand(value = LuceneCliStrings.LUCENE_LIST_INDEX,
      help = LuceneCliStrings.LUCENE_LIST_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public Result listIndex(@CliOption(key = LuceneCliStrings.LUCENE_LIST_INDEX__STATS,
      specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
      help = LuceneCliStrings.LUCENE_LIST_INDEX__STATS__HELP) final boolean stats) {

    authorize(Resource.CLUSTER, Operation.READ, LucenePermission.TARGET);
    return toTabularResult(getIndexListing(), stats);
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexListing() {
    final Execution functionExecutor = getMembersFunctionExecutor(getAllMembers());

    if (functionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) functionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector resultsCollector =
        functionExecutor.execute(new LuceneListIndexFunction());
    final List<Set<LuceneIndexDetails>> results =
        (List<Set<LuceneIndexDetails>>) resultsCollector.getResult();

    List<LuceneIndexDetails> sortedResults =
        results.stream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
    LinkedHashSet<LuceneIndexDetails> uniqResults = new LinkedHashSet<>();
    uniqResults.addAll(sortedResults);
    sortedResults.clear();
    sortedResults.addAll(uniqResults);
    return sortedResults;
  }

  protected Result toTabularResult(final List<LuceneIndexDetails> indexDetailsList, boolean stats) {
    if (!indexDetailsList.isEmpty()) {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();

      for (final LuceneIndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Index Name", indexDetails.getIndexName());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Server Name", indexDetails.getServerName());
        indexData.accumulate("Indexed Fields", indexDetails.getSearchableFieldNamesString());
        indexData.accumulate("Field Analyzer", indexDetails.getFieldAnalyzersString());
        indexData.accumulate("Serializer", indexDetails.getSerializerString());
        indexData.accumulate("Status", indexDetails.getStatus().toString());

        if (stats) {
          LuceneIndexStatus luceneIndexStatus = indexDetails.getStatus();
          if (luceneIndexStatus == LuceneIndexStatus.NOT_INITIALIZED
              || luceneIndexStatus == LuceneIndexStatus.INDEXING_IN_PROGRESS) {
            indexData.accumulate("Query Executions", "NA");
            indexData.accumulate("Updates", "NA");
            indexData.accumulate("Commits", "NA");
            indexData.accumulate("Documents", "NA");
          } else {
            indexData.accumulate("Query Executions",
                indexDetails.getIndexStats().get("queryExecutions"));
            indexData.accumulate("Updates", indexDetails.getIndexStats().get("updates"));
            indexData.accumulate("Commits", indexDetails.getIndexStats().get("commits"));
            indexData.accumulate("Documents", indexDetails.getIndexStats().get("documents"));
          }
        }
      }
      return ResultBuilder.buildResult(indexData);
    } else {
      return ResultBuilder
          .createInfoResult(LuceneCliStrings.LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

  /**
   * On the server, we also verify the resource operation permissions CLUSTER:WRITE:DISK
   */
  @CliCommand(value = LuceneCliStrings.LUCENE_CREATE_INDEX,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  public Result createIndex(@CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__REGION_HELP) final String regionPath,

      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, mandatory = true,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD_HELP) final String[] fields,

      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER_HELP) final String[] analyzers,
      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER_HELP) final String serializer)
      throws CommandResultException {

    Result result;
    // Every lucene index potentially writes to disk.
    authorize(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);

    final InternalCache cache = (InternalCache) getCache();

    // trim fields for any leading trailing spaces.
    String[] trimmedFields = Arrays.stream(fields).map(String::trim).toArray(String[]::new);
    LuceneIndexInfo indexInfo =
        new LuceneIndexInfo(indexName, regionPath, trimmedFields, analyzers, serializer);

    final ResultCollector<?, ?> rc =
        this.executeFunctionOnAllMembers(createIndexFunction, indexInfo);
    final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();
    final XmlEntity xmlEntity = funcResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
    final TabularResultData tabularResult = ResultBuilder.createTabularResultData();
    for (final CliFunctionResult cliFunctionResult : funcResults) {
      tabularResult.accumulate("Member", cliFunctionResult.getMemberIdOrName());

      if (cliFunctionResult.isSuccessful()) {
        tabularResult.accumulate("Status", "Successfully created lucene index");
      } else {
        tabularResult.accumulate("Status", "Failed: " + cliFunctionResult.getMessage());
      }
    }

    result = ResultBuilder.buildResult(tabularResult);
    if (xmlEntity != null) {
      persistClusterConfiguration(result, () -> {
        ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
            .addXmlEntity(xmlEntity, null);
      });
    }
    return result;
  }

  @CliCommand(value = LuceneCliStrings.LUCENE_DESCRIBE_INDEX,
      help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public Result describeIndex(
      @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
          help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__REGION_HELP) final String regionPath)
      throws Exception {

    authorize(Resource.CLUSTER, Operation.READ, LucenePermission.TARGET);
    LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath);
    return toTabularResult(getIndexDetails(indexInfo), true);
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexDetails(LuceneIndexInfo indexInfo) throws Exception {
    final ResultCollector<?, ?> rc =
        executeFunctionOnRegion(describeIndexFunction, indexInfo, true);
    final List<LuceneIndexDetails> funcResults = (List<LuceneIndexDetails>) rc.getResult();
    return funcResults.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Internally, we verify the resource operation permissions DATA:READ:[RegionName]
   */
  @CliCommand(value = LuceneCliStrings.LUCENE_SEARCH_INDEX,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public Result searchIndex(@CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_SEARCH_INDEX__REGION_HELP) final String regionPath,

      @CliOption(
          key = {LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,
              LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRINGS},
          mandatory = true,
          help = LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING__HELP) final String queryString,

      @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, mandatory = true,
          help = LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD__HELP) final String defaultField,

      @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT, unspecifiedDefaultValue = "-1",
          help = LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT__HELP) final int limit,

      @CliOption(key = LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY,
          unspecifiedDefaultValue = "false",
          help = LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY__HELP) boolean keysOnly)
      throws Exception {
    authorize(Resource.DATA, Operation.READ, regionPath);
    LuceneQueryInfo queryInfo =
        new LuceneQueryInfo(indexName, regionPath, queryString, defaultField, limit, keysOnly);
    int pageSize = Integer.MAX_VALUE;
    searchResults = getSearchResults(queryInfo);
    return displayResults(pageSize, keysOnly);

  }

  @CliCommand(value = LuceneCliStrings.LUCENE_DESTROY_INDEX,
      help = LuceneCliStrings.LUCENE_DESTROY_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public Result destroyIndex(@CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME,
      help = LuceneCliStrings.LUCENE_DESTROY_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_DESTROY_INDEX__REGION_HELP) final String regionPath) {

    if (indexName != null && StringUtils.isEmpty(indexName)) {
      return ResultBuilder.createInfoResult(
          CliStrings.format(LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__INDEX_CANNOT_BE_EMPTY));
    }

    authorize(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);

    // Get members >= Geode 1.8 (when the new destroy code path went into the product)
    Set<DistributedMember> validVersionMembers =
        getNormalMembersWithSameOrNewerVersion(Version.GEODE_170);
    if (validVersionMembers.isEmpty()) {
      return ResultBuilder.createInfoResult(CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULD_NOT_FIND__MEMBERS_GREATER_THAN_VERSION_0,
          Version.GEODE_170));
    }

    // Execute the destroy index function
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath);
    ResultCollector<?, ?> rc =
        executeFunction(destroyIndexFunction, indexInfo, validVersionMembers);

    // Get the result
    List<CliFunctionResult> cliFunctionResults = (List<CliFunctionResult>) rc.getResult();
    Result result = getDestroyIndexResult(cliFunctionResults, indexName, regionPath);

    // Get and process the xml entity
    XmlEntity xmlEntity = findXmlEntity(cliFunctionResults);
    if (xmlEntity != null) {
      persistClusterConfiguration(result, () -> {
        // Delete the xml entity to remove the index(es) in all groups
        ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
            .deleteXmlEntity(xmlEntity, null);
      });
    }

    return result;
  }

  private Result getDestroyIndexResult(List<CliFunctionResult> cliFunctionResults, String indexName,
      String regionPath) {
    final TabularResultData tabularResult = ResultBuilder.createTabularResultData();
    for (CliFunctionResult cliFunctionResult : cliFunctionResults) {
      tabularResult.accumulate("Member", cliFunctionResult.getMemberIdOrName());
      if (cliFunctionResult.isSuccessful()) {
        tabularResult.accumulate("Status",
            indexName == null
                ? CliStrings.format(
                    LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
                    new Object[] {regionPath})
                : CliStrings.format(
                    LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
                    new Object[] {indexName, regionPath}));
      } else {
        tabularResult.accumulate("Status", cliFunctionResult.getMessage());
      }
    }
    return ResultBuilder.buildResult(tabularResult);
  }

  private Result displayResults(int pageSize, boolean keysOnly) throws Exception {
    if (searchResults.size() == 0) {
      return ResultBuilder
          .createInfoResult(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE);
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
        case "n": {
          if (currentPage == totalPages) {
            gfsh.printAsInfo("No more results to display.");
            step = gfsh.interact("Press p to move to last page and q to quit.");
            skipDisplay = true;
            continue;
          }

          if (skipDisplay) {
            skipDisplay = false;
          } else {
            currentPage++;
            int current = fromIndex;
            fromIndex = toIndex;
            toIndex = (pageSize + fromIndex >= searchResults.size()) ? searchResults.size()
                : pageSize + fromIndex;
          }
          break;
        }
        case "p": {
          if (currentPage == 1) {
            gfsh.printAsInfo("At the top of the search results.");
            step = gfsh.interact("Press n to move to the first page and q to quit.");
            skipDisplay = true;
            continue;
          }

          if (skipDisplay) {
            skipDisplay = false;
          } else {
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
    } while (true);
  }

  protected Gfsh initGfsh() {
    return Gfsh.getCurrentInstance();
  }

  private List<LuceneSearchResults> getSearchResults(final LuceneQueryInfo queryInfo)
      throws Exception {
    final String[] groups = {};
    final ResultCollector<?, ?> rc = this.executeSearch(queryInfo);
    final List<Set<LuceneSearchResults>> functionResults =
        (List<Set<LuceneSearchResults>>) rc.getResult();

    return functionResults.stream().flatMap(Collection::stream).sorted()
        .collect(Collectors.toList());
  }

  private Result getResults(int fromIndex, int toIndex, boolean keysonly) throws Exception {
    final TabularResultData data = ResultBuilder.createTabularResultData();
    for (int i = fromIndex; i < toIndex; i++) {
      if (!searchResults.get(i).getExceptionFlag()) {
        data.accumulate("key", searchResults.get(i).getKey());
        if (!keysonly) {
          data.accumulate("value", searchResults.get(i).getValue());
          data.accumulate("score", searchResults.get(i).getScore());
        }
      } else {
        throw new UserErrorException(searchResults.get(i).getExceptionMessage());
      }
    }
    return ResultBuilder.buildResult(data);
  }

  protected ResultCollector<?, ?> executeFunctionOnAllMembers(Function function,
      final LuceneFunctionSerializable functionArguments) throws IllegalArgumentException {
    Set<DistributedMember> targetMembers = getAllNormalMembers();
    return executeFunction(function, functionArguments, targetMembers);
  }

  protected ResultCollector<?, ?> executeSearch(final LuceneQueryInfo queryInfo) throws Exception {
    return executeFunctionOnRegion(searchIndexFunction, queryInfo, false);
  }

  protected ResultCollector<?, ?> executeFunctionOnRegion(Function function,
      LuceneFunctionSerializable functionArguments, boolean returnAllMembers) {
    Set<DistributedMember> targetMembers = CliUtil.getRegionAssociatedMembers(
        functionArguments.getRegionPath(), (InternalCache) getCache(), returnAllMembers);
    if (targetMembers.isEmpty()) {
      throw new UserErrorException(CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULDNOT_FIND_MEMBERS_FOR_REGION_0,
          new Object[] {functionArguments.getRegionPath()}));
    }
    return executeFunction(function, functionArguments, targetMembers);
  }

  @CliAvailabilityIndicator({LuceneCliStrings.LUCENE_SEARCH_INDEX,
      LuceneCliStrings.LUCENE_CREATE_INDEX, LuceneCliStrings.LUCENE_DESCRIBE_INDEX,
      LuceneCliStrings.LUCENE_LIST_INDEX, LuceneCliStrings.LUCENE_DESTROY_INDEX})
  public boolean indexCommandsAvailable() {
    Gfsh gfsh = Gfsh.getCurrentInstance();

    // command should always be available on the server
    if (gfsh == null) {
      return true;
    }

    // if in gfshVM, only when gfsh is connected and ready
    return gfsh.isConnectedAndReady();
  }
}
