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
package org.apache.geode.management.internal.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.json.QueryResultFormatter;

/**
 * This function is executed on one or multiple members based on the member input to
 * DataQueryEngine.queryData()
 */
@SuppressWarnings({"deprecation", "unchecked"})
public class QueryDataFunction implements Function, InternalEntity {

  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();

  /**
   * Limit of collection length to be serialized in JSON format.
   */
  public static final int DEFAULT_COLLECTION_ELEMENT_LIMIT = 100;

  private static final String MEMBER_KEY = "member";
  private static final String RESULT_KEY = "result";
  private static final String NO_DATA_FOUND = "No Data Found";
  private static final int DISPLAY_MEMBERWISE = 0;
  private static final int QUERY = 1;
  private static final int REGION = 2;
  private static final int LIMIT = 3;
  private static final int QUERY_RESULTSET_LIMIT = 4;
  private static final int QUERY_COLLECTIONS_DEPTH = 5;
  private static final String SELECT_EXPR = "\\s*SELECT\\s+.+\\s+FROM.+";
  private static final Pattern SELECT_EXPR_PATTERN =
      Pattern.compile(SELECT_EXPR, Pattern.CASE_INSENSITIVE);
  private static final String SELECT_WITH_LIMIT_EXPR =
      "\\s*SELECT\\s+.+\\s+FROM(\\s+|(.*\\s+))LIMIT\\s+[0-9]+.*";
  private static final Pattern SELECT_WITH_LIMIT_EXPR_PATTERN =
      Pattern.compile(SELECT_WITH_LIMIT_EXPR, Pattern.CASE_INSENSITIVE);

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(final FunctionContext context) {
    Object[] functionArgs = (Object[]) context.getArguments();
    boolean showMember = (Boolean) functionArgs[DISPLAY_MEMBERWISE];
    String queryString = (String) functionArgs[QUERY];
    String regionName = (String) functionArgs[REGION];
    int limit = (Integer) functionArgs[LIMIT];

    int queryResultSetLimit = (Integer) functionArgs[QUERY_RESULTSET_LIMIT];

    int queryCollectionsDepth = (Integer) functionArgs[QUERY_COLLECTIONS_DEPTH];

    try {
      context.getResultSender().lastResult(selectWithType(context, queryString, showMember,
          regionName, limit, queryResultSetLimit, queryCollectionsDepth));
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

  @Override
  public String getId() {
    return ManagementConstants.QUERY_DATA_FUNCTION;
  }

  // return the compressed result data
  private byte[] selectWithType(final FunctionContext context, String queryString,
      final boolean showMember, final String regionName, final int limit,
      final int queryResultSetLimit, final int queryCollectionsDepth) throws Exception {
    InternalCache cache = (InternalCache) context.getCache();
    Function localQueryFunc = new LocalQueryFunction("LocalQueryFunction", regionName, showMember)
        .setOptimizeForWrite(true);
    queryString = applyLimitClause(queryString, limit, queryResultSetLimit);

    try {
      QueryResultFormatter result = new QueryResultFormatter(queryCollectionsDepth);

      Region region = cache.getRegion(regionName);

      if (region == null) {
        throw new Exception(String.format("Cannot find region %s in member %s", regionName,
            cache.getDistributedSystem().getDistributedMember().getId()));
      }

      Object results = null;

      boolean noDataFound = true;

      if (region.getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
        QueryService queryService = cache.getQueryService();

        Query query = queryService.newQuery(queryString);
        results = query.execute();

      } else {
        ResultCollector rcollector;

        PartitionedRegion parRegion =
            PartitionedRegionHelper.getPartitionedRegion(regionName, cache);
        if (parRegion != null && showMember) {
          if (parRegion.isDataStore()) {

            Set<BucketRegion> localPrimaryBucketRegions =
                parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
            Set<Integer> localPrimaryBucketSet = new HashSet<>();
            for (BucketRegion bRegion : localPrimaryBucketRegions) {
              localPrimaryBucketSet.add(bRegion.getId());
            }
            LocalDataSet lds =
                new LocalDataSet(parRegion, localPrimaryBucketSet);
            DefaultQuery query = (DefaultQuery) cache.getQueryService().newQuery(queryString);
            final ExecutionContext executionContext = new QueryExecutionContext(null, cache, query);
            results = lds.executeQuery(query, executionContext, null, localPrimaryBucketSet);
          }
        } else {
          rcollector = FunctionService.onRegion(cache.getRegion(regionName))
              .setArguments(queryString).execute(localQueryFunc);
          results = rcollector.getResult();
        }
      }

      if (results != null && results instanceof SelectResults) {

        SelectResults selectResults = (SelectResults) results;
        for (Object object : selectResults) {
          result.add(RESULT_KEY, object);
          noDataFound = false;
        }
      } else if (results != null && results instanceof ArrayList) {
        ArrayList listResults = (ArrayList) results;
        ArrayList actualResult = (ArrayList) listResults.get(0);
        for (Object object : actualResult) {
          result.add(RESULT_KEY, object);
          noDataFound = false;
        }
      }

      if (!noDataFound && showMember) {
        result.add(MEMBER_KEY, cache.getDistributedSystem().getDistributedMember().getId());
      }

      if (noDataFound) {
        return BeanUtilFuncs
            .compress(new DataQueryEngine.JsonisedErrorMessage(NO_DATA_FOUND).toString());
      }
      return BeanUtilFuncs.compress(result.toString());
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Matches the input query with query with limit pattern. If limit is found in input query this
   * function ignores. Else it will append a default limit .. 1000 If input limit is 0 then also it
   * will append default limit of 1000
   *
   * @param query input query
   * @param limit limit on the result set
   *
   * @return a string having limit clause
   */
  protected static String applyLimitClause(final String query, int limit,
      final int queryResultSetLimit) {
    String[] lines = query.split(System.lineSeparator());
    List<String> queryStrings = new ArrayList();
    for (String line : lines) {
      // remove the comments
      if (!line.startsWith("--") && line.length() > 0) {
        queryStrings.add(line);
      }
    }
    if (queryStrings.isEmpty()) {
      throw new IllegalArgumentException("invalid query: " + query);
    }

    String queryString = String.join(" ", queryStrings);

    Matcher matcher = SELECT_EXPR_PATTERN.matcher(queryString);

    if (matcher.matches()) {
      Matcher limit_matcher = SELECT_WITH_LIMIT_EXPR_PATTERN.matcher(queryString);
      boolean queryAlreadyHasLimitClause = limit_matcher.matches();

      if (!queryAlreadyHasLimitClause) {
        if (limit == 0) {
          limit = queryResultSetLimit;
        }
        return queryString + " LIMIT " + limit;
      }
    }
    return queryString;
  }



  /**
   * Function to gather data locally. This function is required to execute query with region context
   */
  private class LocalQueryFunction implements InternalFunction {

    private static final long serialVersionUID = 1L;

    private final String id;

    private boolean optimizeForWrite = false;
    private final boolean showMembers;
    private final String regionName;

    public LocalQueryFunction(final String id, final String regionName, final boolean showMembers) {
      this.id = id;
      this.regionName = regionName;
      this.showMembers = showMembers;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return optimizeForWrite;
    }

    public LocalQueryFunction setOptimizeForWrite(final boolean optimizeForWrite) {
      this.optimizeForWrite = optimizeForWrite;
      return this;
    }

    @Override
    public void execute(final FunctionContext context) {
      InternalCache cache = (InternalCache) context.getCache();
      QueryService queryService = cache.getQueryService();
      String qstr = (String) context.getArguments();
      Region r = cache.getRegion(regionName);
      try {
        Query query = queryService.newQuery(qstr);
        SelectResults sr;
        if (r.getAttributes().getPartitionAttributes() != null && showMembers) {
          sr = (SelectResults) query.execute((RegionFunctionContext) context);
          context.getResultSender().lastResult(sr.asList());
        } else {
          sr = (SelectResults) query.execute();
          context.getResultSender().lastResult(sr.asList());
        }

      } catch (Exception e) {
        throw new FunctionException(e);
      }
    }

    @Override
    public String getId() {
      return id;
    }
  }
}
