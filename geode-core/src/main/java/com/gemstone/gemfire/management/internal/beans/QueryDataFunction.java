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
package com.gemstone.gemfire.management.internal.beans;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.cli.commands.DataCommands;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.json.TypedJson;

/**
 * This function is executed on one or multiple members based on the member
 * input to DistributedSystemMXBean.queryData()
 * 
 * 
 */
public class QueryDataFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();
  
  @Override
  public boolean hasResult() {
    return true;
  }

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Object[] functionArgs = (Object[]) context.getArguments();
    boolean showMember = (Boolean) functionArgs[DISPLAY_MEMBERWISE];
    String queryString = (String) functionArgs[QUERY];
    String regionName = (String) functionArgs[REGION];
    int limit = (Integer) functionArgs[LIMIT];
    
    int queryResultSetLimit = (Integer) functionArgs[QUERY_RESULTSET_LIMIT];
    
    int queryCollectionsDepth = (Integer) functionArgs[QUERY_COLLECTIONS_DEPTH];
    
    
    try {
      context.getResultSender().lastResult(
          selectWithType(context, queryString, showMember, regionName, limit, queryResultSetLimit,
              queryCollectionsDepth));
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }

  }

  @Override
  public String getId() {
    return ManagementConstants.QUERY_DATA_FUNCTION;
  }

  @SuppressWarnings( { "unchecked" })
  public QueryDataFunctionResult selectWithType(FunctionContext context, String queryString, boolean showMember,
      String regionName, int limit, int queryResultSetLimit, int queryCollectionsDepth) throws Exception {

    Cache cache = CacheFactory.getAnyInstance();

    Function loclQueryFunc = new LocalQueryFunction("LocalQueryFunction", regionName, showMember)
        .setOptimizeForWrite(true);

    queryString = applyLimitClause(queryString, limit, queryResultSetLimit);

    try {

      TypedJson result = new TypedJson(queryCollectionsDepth);

      Region region = cache.getRegion(regionName);

      if (region == null) {
        throw new Exception(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBER.toLocalizedString(regionName,
            cache.getDistributedSystem().getDistributedMember().getId()));
      }

      Object results = null;
      
      boolean noDataFound = true;

      if (region.getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
        QueryService queryService = cache.getQueryService();

        Query query = queryService.newQuery(queryString);
        results = query.execute();

      } else {
        ResultCollector rcollector = null;
        
        PartitionedRegion parRegion = PartitionedRegionHelper.getPartitionedRegion(regionName, cache);
        if(parRegion != null && showMember){
          if(parRegion.isDataStore()){
            
            Set<BucketRegion> localPrimaryBucketRegions = parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
            Set<Integer> localPrimaryBucketSet = new HashSet<Integer>();
            for (BucketRegion bRegion : localPrimaryBucketRegions) {
              localPrimaryBucketSet.add(bRegion.getId());
            }
            LocalDataSet lds = new LocalDataSet(parRegion, localPrimaryBucketSet);
            DefaultQuery query = (DefaultQuery)cache.getQueryService().newQuery(
                queryString);
            SelectResults selectResults = (SelectResults)lds.executeQuery(query, null, localPrimaryBucketSet);
            results = selectResults;
          }
        }else{
          rcollector = FunctionService.onRegion(cache.getRegion(regionName)).withArgs(queryString)
              .execute(loclQueryFunc);
          results = (ArrayList) rcollector.getResult();
        }

        
      }

      if (results != null && results instanceof SelectResults) {

        SelectResults selectResults = (SelectResults) results;
        for (Iterator iter = selectResults.iterator(); iter.hasNext();) {
          Object object = iter.next();
          result.add(RESULT_KEY,object);
          noDataFound = false;
        }
      } else if (results != null && results instanceof ArrayList) {
        ArrayList listResults = (ArrayList) results;
        ArrayList actualResult = (ArrayList)listResults.get(0);
        for (Object object : actualResult) {
          result.add(RESULT_KEY, object);
          noDataFound = false;
        }
      } 
      
      if (!noDataFound && showMember) {
        result.add(MEMBER_KEY,cache.getDistributedSystem().getDistributedMember().getId());
      }

      if (noDataFound) {
        return new QueryDataFunctionResult(QUERY_EXEC_SUCCESS, BeanUtilFuncs.compress(new JsonisedErroMessage(NO_DATA_FOUND).toString()));
      }
      return new QueryDataFunctionResult(QUERY_EXEC_SUCCESS, BeanUtilFuncs.compress(result.toString()));
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } finally {

    }

  }


  /**
   * Matches the input query with query with limit pattern. If limit is found in
   * input query this function ignores. Else it will append a default limit ..
   * 1000 If input limit is 0 then also it will append default limit of 1000
   * 
   * @param query
   *          input query
   * @param limit
   *          limit on the result set
   * @return a string having limit clause
   */
  static String applyLimitClause(String query, int limit, int queryResultSetLimit) {
	  
    Matcher matcher = SELECT_EXPR_PATTERN.matcher(query); 

    if (matcher.matches()) {
      Matcher limit_matcher = SELECT_WITH_LIMIT_EXPR_PATTERN.matcher(query);
      boolean matchResult = limit_matcher.matches();

      if (!matchResult) {
        if (limit == 0) {
          limit = queryResultSetLimit;
        }
        String result = new String(query);
        result += " LIMIT " + limit;
        return result;
      }
    }
    return query;
  }

  @SuppressWarnings( { "unchecked" })
  static Object callFunction(Object functionArgs, Set<DistributedMember> members, boolean zipResult) throws Exception {

    try {
      if (members.size() == 1) {
        DistributedMember member = members.iterator().next();
        ResultCollector collector = FunctionService.onMember(member).withArgs(functionArgs).execute(
            ManagementConstants.QUERY_DATA_FUNCTION);
        List list = (List) collector.getResult();
        Object object = null;
        if (list.size() > 0) {
          object = list.get(0);
        }

        if (object instanceof Throwable) {
          Throwable error = (Throwable) object;
          throw error;
        }

        QueryDataFunctionResult result = (QueryDataFunctionResult) object;
        if (zipResult) { // The result is already compressed
          return result.compressedBytes;
        } else {
          Object[] functionArgsList = (Object[]) functionArgs;
          boolean showMember = (Boolean) functionArgsList[DISPLAY_MEMBERWISE];
          if (showMember) {// Added to show a single member similar to multiple
                           // member.
            // Note , if no member is selected this is the code path executed. A
            // random associated member is chosen.
            List<String> decompressedList = new ArrayList<String>();
            decompressedList.add(BeanUtilFuncs.decompress(result.compressedBytes));
            return wrapResult(decompressedList.toString());
          }
          return BeanUtilFuncs.decompress(result.compressedBytes);
        }

      } else { // More than 1 Member
        ResultCollector coll = FunctionService.onMembers(members).withArgs(functionArgs).execute(
            ManagementConstants.QUERY_DATA_FUNCTION);

        List list = (List) coll.getResult();
        Object object = list.get(0);
        if (object instanceof Throwable) {
          Throwable error = (Throwable) object;
          throw error;
        }

        Iterator<QueryDataFunctionResult> it = list.iterator();
        List<String> decompressedList = new ArrayList<String>();

        while (it.hasNext()) {

          String decompressedStr = null;
          decompressedStr = BeanUtilFuncs.decompress(it.next().compressedBytes);
          decompressedList.add(decompressedStr);

        }
        if (zipResult) {
          return BeanUtilFuncs.compress(wrapResult(decompressedList.toString()));
        } else {
          return wrapResult(decompressedList.toString());
        }

      }
    } catch (FunctionException fe) {
      throw new Exception(ManagementStrings.QUERY__MSG__QUERY_EXEC.toLocalizedString(fe.getMessage()));
    } catch (IOException e) {
      throw new Exception(ManagementStrings.QUERY__MSG__QUERY_EXEC.toLocalizedString(e.getMessage()));
    } catch (Exception e) {
      throw new Exception(ManagementStrings.QUERY__MSG__QUERY_EXEC.toLocalizedString(e.getMessage()));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable e) {
      SystemFailure.checkFailure();
      throw new Exception(ManagementStrings.QUERY__MSG__QUERY_EXEC.toLocalizedString(e.getMessage()));
    }
  }

  static String wrapResult(String str) {
    StringWriter w = new StringWriter();
    synchronized (w.getBuffer()) {
      w.write("{\"result\":");
      w.write(str);
      w.write("}");
      return w.toString();
    }
  }
  
 

  public static Object queryData(String query, String members, int limit, boolean zipResult, int queryResultSetLimit, int queryCollectionsDepth)  throws Exception {

    if (query == null || query.isEmpty()) {
      return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__QUERY_EMPTY.toLocalizedString()).toString();
    }

    Set<DistributedMember> inputMembers = null;
    if (members != null && !members.trim().isEmpty()) {
      inputMembers = new HashSet<DistributedMember>();
      StringTokenizer st = new StringTokenizer(members, ",");
      while (st.hasMoreTokens()) {
        String member = st.nextToken();
        DistributedMember distributedMember = BeanUtilFuncs.getDistributedMemberByNameOrId(member);
        inputMembers.add(distributedMember);
        if (distributedMember == null) {
          return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__INVALID_MEMBER.toLocalizedString(member)).toString();
        }
      }
    }

    Cache cache = CacheFactory.getAnyInstance();
    try {

      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
      Set<String> regionsInQuery = compileQuery(cache, query);
      
      // Validate region existance
      if (regionsInQuery.size() > 0) {
        for (String regionPath : regionsInQuery) {
          DistributedRegionMXBean regionMBean = service.getDistributedRegionMXBean(regionPath);
          if (regionMBean == null) {
            return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND.toLocalizedString(regionPath)).toString();
          } else {
            Set<DistributedMember> associatedMembers = DataCommands
                .getRegionAssociatedMembers(regionPath, cache, true);

            if (inputMembers != null && inputMembers.size() > 0) {
              if (!associatedMembers.containsAll(inputMembers)) {
                return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBERS
                    .toLocalizedString(regionPath)).toString();
              }
            }            
          }

        }
      } else {
        return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__INVALID_QUERY
            .toLocalizedString("Region mentioned in query probably missing /")).toString();
      }

      // Validate
      if (regionsInQuery.size() > 1 && inputMembers == null) {
        for (String regionPath : regionsInQuery) {
          DistributedRegionMXBean regionMBean = service.getDistributedRegionMXBean(regionPath);

          if (regionMBean.getRegionType().equals(DataPolicy.PARTITION.toString())
              || regionMBean.getRegionType().equals(DataPolicy.PERSISTENT_PARTITION.toString())) {
            return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__JOIN_OP_EX.toLocalizedString()).toString();
          }
        }
      }


      String randomRegion = regionsInQuery.iterator().next();
      
      Set<DistributedMember> associatedMembers = DataCommands.getQueryRegionsAssociatedMembers(regionsInQuery, cache,
          false);// First available member


      if (associatedMembers != null && associatedMembers.size() > 0) {
        Object[] functionArgs = new Object[6];
        if (inputMembers != null && inputMembers.size() > 0) {// on input
          // members

          functionArgs[DISPLAY_MEMBERWISE] = true;
          functionArgs[QUERY] = query;
          functionArgs[REGION] = randomRegion;
          functionArgs[LIMIT] = limit;
          functionArgs[QUERY_RESULTSET_LIMIT] = queryResultSetLimit;
          functionArgs[QUERY_COLLECTIONS_DEPTH] = queryCollectionsDepth;
          Object result = QueryDataFunction.callFunction(functionArgs, inputMembers, zipResult);
          return result;
        } else { // Query on any random member
          functionArgs[DISPLAY_MEMBERWISE] = false;
          functionArgs[QUERY] = query;
          functionArgs[REGION] = randomRegion;
          functionArgs[LIMIT] = limit;
          functionArgs[QUERY_RESULTSET_LIMIT] = queryResultSetLimit;
          functionArgs[QUERY_COLLECTIONS_DEPTH] = queryCollectionsDepth;
          Object result = QueryDataFunction.callFunction(functionArgs, associatedMembers, zipResult);
          return result;
        }

      } else {
        return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND.toLocalizedString(regionsInQuery
            .toString())).toString();
      }

    } catch (QueryInvalidException qe) {
      return new JsonisedErroMessage(ManagementStrings.QUERY__MSG__INVALID_QUERY.toLocalizedString(qe.getMessage())).toString();
    } 
  }
  
  
  private static class JsonisedErroMessage {

    private static String message = "message";

    private GfJsonObject gFJsonObject = new GfJsonObject();

    public JsonisedErroMessage(String errorMessage) throws Exception {
      try {
        gFJsonObject.put(message, errorMessage);
      } catch (GfJsonException e) {
        throw new Exception(e);
      }
    }

    public String toString() {
      return gFJsonObject.toString();
    }

  }

  /**
   * Compile the query and return a set of regions involved in the query It
   * throws an QueryInvalidException if the query is not proper
   * 
   * @param cache
   *          current cache
   * @param query
   *          input query
   * @return a set of regions involved in the query
   * @throws QueryInvalidException
   */
  @SuppressWarnings("deprecation")
  public static Set<String> compileQuery(Cache cache, String query) throws QueryInvalidException {
    QCompiler compiler = new QCompiler();
    Set<String> regionsInQuery = null;
    try {
      CompiledValue compiledQuery = compiler.compileQuery(query);
      Set<String> regions = new HashSet<String>();
      compiledQuery.getRegionsInQuery(regions, null);
      regionsInQuery = Collections.unmodifiableSet(regions);
      return regionsInQuery;
    } catch (QueryInvalidException qe) {
      logger.error("{} Failed, Error {}", query, qe.getMessage(), qe);
      throw qe;
    }
  }

  /**
   * Function to gather data locally. This function is required to execute query
   * with region context
   * 
   * 
   */
  private class LocalQueryFunction extends FunctionAdapter {

    private static final long serialVersionUID = 1L;

    private boolean optimizeForWrite = false;

    private boolean showMembers = false;

    private String regionName;

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    private final String id;

    @Override
    public boolean optimizeForWrite() {
      return optimizeForWrite;
    }

    public LocalQueryFunction setOptimizeForWrite(boolean optimizeForWrite) {
      this.optimizeForWrite = optimizeForWrite;
      return this;
    }

    public LocalQueryFunction(String id, String regionName, boolean showMembers) {
      super();
      this.id = id;
      this.regionName = regionName;
      this.showMembers = showMembers;

    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(FunctionContext context) {
      Cache cache = CacheFactory.getAnyInstance();
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
      return this.id;
    }
  }

  private static String MEMBER_KEY = "member";

  private static String RESULT_KEY = "result";

  private static String NO_DATA_FOUND = "No Data Found";
  
  private static String QUERY_EXEC_SUCCESS = "Query Executed Successfuly";

  private static int DISPLAY_MEMBERWISE = 0;

  private static int QUERY = 1;

  private static int REGION = 2;

  private static int LIMIT = 3;
  
  private static int QUERY_RESULTSET_LIMIT = 4;
  
  private static int QUERY_COLLECTIONS_DEPTH = 5;

  static final String SELECT_EXPR = "\\s*SELECT\\s+.+\\s+FROM\\s+.+";

  static Pattern SELECT_EXPR_PATTERN = Pattern.compile(SELECT_EXPR, Pattern.CASE_INSENSITIVE);

  static final String SELECT_WITH_LIMIT_EXPR = "\\s*SELECT\\s+.+\\s+FROM(\\s+|(.*\\s+))LIMIT\\s+[0-9]+.*";

  static Pattern SELECT_WITH_LIMIT_EXPR_PATTERN = Pattern.compile(SELECT_WITH_LIMIT_EXPR, Pattern.CASE_INSENSITIVE);


  public static class QueryDataFunctionResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String message;
    private final byte[] compressedBytes;

    public QueryDataFunctionResult(String message, byte[] compressedBytes) {
      this.message = message;
      this.compressedBytes = compressedBytes;
    }

    /**
     * @return the message
     */
    public String getMessage() {
      return message;
    }

    /**
     * @return the compressedBytes
     */
    public byte[] getCompressedBytes() {
      return compressedBytes;
    }
  }
}
