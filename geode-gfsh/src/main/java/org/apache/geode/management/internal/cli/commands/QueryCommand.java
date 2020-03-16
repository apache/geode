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

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class QueryCommand extends GfshCommand {
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = "query", help = CliStrings.QUERY__HELP)
  @CliMetaData(interceptor = "org.apache.geode.management.internal.cli.commands.QueryInterceptor")
  public ResultModel query(
      @CliOption(key = CliStrings.QUERY__QUERY, help = CliStrings.QUERY__QUERY__HELP,
          mandatory = true) final String query,
      @CliOption(key = "file", help = "File in which to output the results.",
          optionContext = ConverterHint.FILE) final File outputFile,
      @CliOption(key = CliStrings.QUERY__INTERACTIVE, unspecifiedDefaultValue = "false",
          help = CliStrings.QUERY__INTERACTIVE__HELP) final boolean interactive) {
    DataCommandResult dataResult = select(query);
    return dataResult.toSelectCommandResult();
  }

  private DataCommandResult select(String query) {
    Cache cache = getCache();
    DataCommandResult dataResult;

    if (StringUtils.isEmpty(query)) {
      dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
          CliStrings.QUERY__MSG__QUERY_EMPTY, false);
      return dataResult;
    }

    boolean limitAdded = false;

    if (!StringUtils.containsIgnoreCase(query, " limit")
        && !StringUtils.containsIgnoreCase(query, " count(")) {
      query = query + " limit " + CommandExecutionContext.getShellFetchSize();
      limitAdded = true;
    }

    QCompiler compiler = new QCompiler();
    Set<String> regionsInQuery;
    try {
      CompiledValue compiledQuery = compiler.compileQuery(query);
      Set<String> regions = new HashSet<>();
      compiledQuery.getRegionsInQuery(regions, null);

      // authorize data read on these regions
      for (String region : regions) {
        authorize(Resource.DATA, Operation.READ, region);
      }

      regionsInQuery = Collections.unmodifiableSet(regions);
      if (regionsInQuery.size() > 0) {
        Set<DistributedMember> members =
            ManagementUtils
                .getQueryRegionsAssociatedMembers(regionsInQuery, (InternalCache) cache, false);
        if (members != null && members.size() > 0) {
          DataCommandFunction function = new DataCommandFunction();
          DataCommandRequest request = new DataCommandRequest();
          request.setCommand(CliStrings.QUERY);
          request.setQuery(query);
          Subject subject = getSubject();
          if (subject != null) {
            request.setPrincipal(subject.getPrincipal());
          }
          dataResult = callFunctionForRegion(request, function, members);
          dataResult.setInputQuery(query);
          if (limitAdded) {
            dataResult.setLimit(CommandExecutionContext.getShellFetchSize());
          }
          return dataResult;
        } else {
          return DataCommandResult.createSelectInfoResult(null, null, -1, null, CliStrings
              .format(CliStrings.QUERY__MSG__REGIONS_NOT_FOUND, regionsInQuery.toString()), false);
        }
      } else {
        return DataCommandResult.createSelectInfoResult(null, null, -1, null,
            CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY,
                "Region mentioned in query probably missing /"),
            false);
      }
    } catch (QueryInvalidException qe) {
      logger.error("{} Failed Error {}", query, qe.getMessage(), qe);
      return DataCommandResult.createSelectInfoResult(null, null, -1, null,
          CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY, qe.getMessage()), false);
    }
  }

  public static DataCommandResult callFunctionForRegion(DataCommandRequest request,
      DataCommandFunction putfn, Set<DistributedMember> members) {

    if (members.size() == 1) {
      DistributedMember member = members.iterator().next();
      @SuppressWarnings("unchecked")
      ResultCollector<Object, List<Object>> collector =
          FunctionService.onMember(member).setArguments(request).execute(putfn);
      List<Object> list = collector.getResult();
      Object object = list.get(0);
      if (object instanceof Throwable) {
        Throwable error = (Throwable) object;
        DataCommandResult result = new DataCommandResult();
        result.setErorr(error);
        result.setErrorString(error.getMessage());
        return result;
      }
      DataCommandResult result = (DataCommandResult) list.get(0);
      result.aggregate(null);
      return result;
    } else {
      @SuppressWarnings("unchecked")
      ResultCollector<Object, List<Object>> collector =
          FunctionService.onMembers(members).setArguments(request).execute(putfn);
      List<Object> list = collector.getResult();
      DataCommandResult result = null;
      for (Object object : list) {
        if (object instanceof Throwable) {
          Throwable error = (Throwable) object;
          result = new DataCommandResult();
          result.setErorr(error);
          result.setErrorString(error.getMessage());
          return result;
        }

        if (result == null) {
          result = (DataCommandResult) object;
          result.aggregate(null);
        } else {
          result.aggregate((DataCommandResult) object);
        }
      }
      return result;
    }
  }
}
