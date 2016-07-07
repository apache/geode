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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails;

import com.gemstone.gemfire.management.internal.cli.domain.IndexInfo;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/**
 * The LuceneIndexCommands class encapsulates all Geode shell (Gfsh) commands related to lucene indexes defined in Geode.
 * </p>
 * @see AbstractCommandsSupport
 * @see LuceneIndexDetails
 * @see LuceneListIndexFunction
 */
@SuppressWarnings("unused")
public class LuceneIndexCommands extends AbstractCommandsSupport {

  @CliCommand(value = LuceneCliStrings.LUCENE_LIST_INDEX, help = LuceneCliStrings.LUCENE_LIST_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic={CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA })
  @ResourceOperation(resource = Resource.CLUSTER, operation = OperationCode.READ)
  public Result listIndex() {
    try {
      return toTabularResult(getIndexListing());
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

  protected Result toTabularResult(final List<LuceneIndexDetails> indexDetailsList) {
    if (!indexDetailsList.isEmpty()) {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();

      for (final LuceneIndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Index Name", indexDetails.getIndexName());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Indexed Fields", indexDetails.getSearchableFieldNamesString());
        indexData.accumulate("Field Analyzer", indexDetails.getFieldAnalyzersString());
      }
      return ResultBuilder.buildResult(indexData);
    }
    else {
      return ResultBuilder.createInfoResult(LuceneCliStrings.LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

}
