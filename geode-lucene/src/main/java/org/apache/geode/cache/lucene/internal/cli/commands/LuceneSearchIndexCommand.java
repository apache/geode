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

package org.apache.geode.cache.lucene.internal.cli.commands;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneQueryInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneSearchResults;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneSearchIndexFunction;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.exceptions.UserErrorException;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission;

public class LuceneSearchIndexCommand extends LuceneCommandBase {

  private static final LuceneSearchIndexFunction searchIndexFunction =
      new LuceneSearchIndexFunction();

  private List<LuceneSearchResults> searchResults = null;

  /**
   * Internally, we verify the resource operation permissions DATA:READ:[RegionName]
   */
  @CliCommand(value = LuceneCliStrings.LUCENE_SEARCH_INDEX,
      help = LuceneCliStrings.LUCENE_SEARCH_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public ResultModel searchIndex(
      @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
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
    authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ, regionPath);
    LuceneQueryInfo queryInfo =
        new LuceneQueryInfo(indexName, regionPath, queryString, defaultField, limit, keysOnly);
    searchResults = getSearchResults(queryInfo);

    return displayResults(getPageSize(), keysOnly);
  }

  protected int getPageSize() {
    return Integer.MAX_VALUE;
  }

  private List<LuceneSearchResults> getSearchResults(final LuceneQueryInfo queryInfo)
      throws Exception {
    final ResultCollector<?, ?> rc = executeSearch(queryInfo);
    final List<Set<LuceneSearchResults>> functionResults =
        (List<Set<LuceneSearchResults>>) rc.getResult();

    return functionResults.stream().flatMap(Collection::stream).sorted()
        .collect(Collectors.toList());
  }

  protected ResultCollector<?, ?> executeSearch(final LuceneQueryInfo queryInfo) throws Exception {
    return executeFunctionOnRegion(searchIndexFunction, queryInfo, false);
  }

  private ResultModel getResults(int fromIndex, int toIndex, boolean keysonly) throws Exception {
    ResultModel result = new ResultModel();
    TabularResultModel table = result.addTable("lucene-indexes");
    for (int i = fromIndex; i < toIndex; i++) {
      if (!searchResults.get(i).getExceptionFlag()) {
        table.accumulate("key", searchResults.get(i).getKey());
        if (!keysonly) {
          table.accumulate("value", searchResults.get(i).getValue());
          table.accumulate("score", Float.toString(searchResults.get(i).getScore()));
        }
      } else {
        throw new UserErrorException(searchResults.get(i).getExceptionMessage());
      }
    }
    return result;
  }

  protected Gfsh initGfsh() {
    return Gfsh.getCurrentInstance();
  }

  private ResultModel displayResults(int pageSize, boolean keysOnly) throws Exception {
    if (searchResults.size() == 0) {
      return ResultModel.createInfo(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE);
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
        ResultModel resultModel = getResults(fromIndex, toIndex, keysOnly);
        if (!pagination) {
          return resultModel;
        }
        Gfsh.println();

        Result commandResult = new CommandResult(resultModel);
        while (commandResult.hasNextLine()) {
          gfsh.printAsInfo(commandResult.nextLine());
        }
        commandResult.resetToFirstLine();

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
          return ResultModel.createInfo("Search complete.");
        default:
          Gfsh.println("Invalid option");
          break;
      }
    } while (true);
  }


  @CliAvailabilityIndicator(LuceneCliStrings.LUCENE_SEARCH_INDEX)
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
