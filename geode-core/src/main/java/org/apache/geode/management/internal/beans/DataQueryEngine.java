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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.util.ManagementUtils;

/**
 * this is used by DistributedSystemBridge.queryData() call. It calls QueryDataFunction on each
 * member in the input
 */
public class DataQueryEngine {
  private static final Logger logger = LogService.getLogger();

  // these numbers represents function argument index
  private static final int DISPLAY_MEMBERWISE = 0;
  private static final int QUERY = 1;
  private static final int REGION = 2;
  private static final int LIMIT = 3;
  private static final int QUERY_RESULTSET_LIMIT = 4;
  private static final int QUERY_COLLECTIONS_DEPTH = 5;

  private final SystemManagementService service;
  private final InternalCache cache;

  public DataQueryEngine(SystemManagementService service, InternalCache cache) {
    this.service = service;
    this.cache = cache;
  }

  public String queryForJsonResult(final String query, final int limit,
      final int queryResultSetLimit, final int queryCollectionsDepth)
      throws Exception {
    return (String) queryData(query, null, limit, false, queryResultSetLimit,
        queryCollectionsDepth);
  }

  public String queryForJsonResult(final String query, String members, final int limit,
      final int queryResultSetLimit, final int queryCollectionsDepth)
      throws Exception {
    return (String) queryData(query, members, limit, false, queryResultSetLimit,
        queryCollectionsDepth);
  }

  public byte[] queryForCompressedResult(final String query, final int limit,
      final int queryResultSetLimit, final int queryCollectionsDepth)
      throws Exception {
    return (byte[]) queryData(query, null, limit, true, queryResultSetLimit, queryCollectionsDepth);
  }

  public Object queryData(final String query, final String members, final int limit,
      final boolean zipResult, final int queryResultSetLimit, final int queryCollectionsDepth)
      throws Exception {

    if (query == null || query.isEmpty()) {
      return new JsonisedErrorMessage("Query is either empty or Null")
          .toString();
    }

    Set<DistributedMember> inputMembers = null;
    if (StringUtils.isNotBlank(members)) {
      inputMembers = ManagementUtils.findMembers(null, members.split(","), cache);
      if (inputMembers.size() == 0) {
        return new JsonisedErrorMessage(
            String.format("Query is invalid due to invalid member : %s", members)).toString();
      }
    }

    try {
      Set<String> regionsInQuery = compileQuery(query);

      // Validate region existence
      if (regionsInQuery.size() > 0) {
        for (String regionPath : regionsInQuery) {
          DistributedRegionMXBean regionMBean = service.getDistributedRegionMXBean(regionPath);
          if (regionMBean == null) {
            return new JsonisedErrorMessage(
                String.format("Cannot find regions %s in any of the members", regionPath))
                    .toString();
          } else {
            Set<DistributedMember> associatedMembers =
                ManagementUtils.getRegionAssociatedMembers(regionPath, cache, true);

            if (inputMembers != null && inputMembers.size() > 0) {
              if (!associatedMembers.containsAll(inputMembers)) {
                return new JsonisedErrorMessage(
                    String.format("Cannot find regions %s in specified members", regionPath))
                        .toString();
              }
            }
          }
        }
      } else {
        return new JsonisedErrorMessage(String.format("Query is invalid due to error : %s",
            "Region mentioned in query probably missing " + SEPARATOR)).toString();
      }

      // Validate
      if (regionsInQuery.size() > 1 && inputMembers == null) {
        for (String regionPath : regionsInQuery) {
          DistributedRegionMXBean regionMBean = service.getDistributedRegionMXBean(regionPath);

          if (regionMBean.getRegionType().equals(DataPolicy.PARTITION.toString())
              || regionMBean.getRegionType().equals(DataPolicy.PERSISTENT_PARTITION.toString())) {
            return new JsonisedErrorMessage(
                "Join operation can only be executed on targeted members, please give member input")
                    .toString();
          }
        }
      }

      String randomRegion = regionsInQuery.iterator().next();

      // get the first available member
      Set<DistributedMember> associatedMembers =
          ManagementUtils.getQueryRegionsAssociatedMembers(regionsInQuery, cache, false);

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
          return callFunction(functionArgs, inputMembers, zipResult);
        } else { // Query on any random member
          functionArgs[DISPLAY_MEMBERWISE] = false;
          functionArgs[QUERY] = query;
          functionArgs[REGION] = randomRegion;
          functionArgs[LIMIT] = limit;
          functionArgs[QUERY_RESULTSET_LIMIT] = queryResultSetLimit;
          functionArgs[QUERY_COLLECTIONS_DEPTH] = queryCollectionsDepth;
          return callFunction(functionArgs, associatedMembers, zipResult);
        }

      } else {
        return new JsonisedErrorMessage(String
            .format("Cannot find regions %s in any of the members", regionsInQuery))
                .toString();
      }

    } catch (QueryInvalidException qe) {
      return new JsonisedErrorMessage(
          String.format("Query is invalid due to error : %s", qe.getMessage()))
              .toString();
    }
  }

  private static Object callFunction(final Object functionArgs,
      final Set<DistributedMember> members, final boolean zipResult) throws Exception {

    try {
      if (members.size() == 1) {
        DistributedMember member = members.iterator().next();
        ResultCollector collector = FunctionService.onMember(member).setArguments(functionArgs)
            .execute(ManagementConstants.QUERY_DATA_FUNCTION);
        List list = (List) collector.getResult();
        Object object = null;
        if (list.size() > 0) {
          object = list.get(0);
        }

        if (object instanceof Throwable) {
          throw (Throwable) object;
        }

        byte[] result = (byte[]) object;
        if (zipResult) { // The result is already compressed
          return result;
        } else {
          Object[] functionArgsList = (Object[]) functionArgs;
          boolean showMember = (Boolean) functionArgsList[DISPLAY_MEMBERWISE];
          if (showMember) {// Added to show a single member similar to multiple
            // member.
            // Note , if no member is selected this is the code path executed. A
            // random associated member is chosen.
            List<String> decompressedList = new ArrayList<>();
            decompressedList.add(BeanUtilFuncs.decompress(result));
            return wrapResult(decompressedList.toString());
          }
          return BeanUtilFuncs.decompress(result);
        }

      } else { // More than 1 Member
        ResultCollector coll = FunctionService.onMembers(members).setArguments(functionArgs)
            .execute(ManagementConstants.QUERY_DATA_FUNCTION);

        List list = (List) coll.getResult();
        Object object = list.get(0);
        if (object instanceof Throwable) {
          throw (Throwable) object;
        }

        Iterator<byte[]> it = list.iterator();
        List<String> decompressedList = new ArrayList<>();

        while (it.hasNext()) {
          String decompressedStr;
          decompressedStr = BeanUtilFuncs.decompress(it.next());
          decompressedList.add(decompressedStr);
        }

        if (zipResult) {
          return BeanUtilFuncs.compress(wrapResult(decompressedList.toString()));
        } else {
          return wrapResult(decompressedList.toString());
        }

      }
    } catch (FunctionException fe) {
      throw new Exception(
          String.format("Query could not be executed due to : %s", fe.getMessage()));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable e) {
      SystemFailure.checkFailure();
      throw new Exception(String.format("Query could not be executed due to : %s", e.getMessage()));
    }
  }

  private static String wrapResult(final String str) {
    StringWriter w = new StringWriter();
    synchronized (w.getBuffer()) {
      w.write("{\"result\":");
      w.write(str);
      w.write("}");
      return w.toString();
    }
  }

  private Set<String> compileQuery(final String query)
      throws QueryInvalidException {
    QCompiler compiler = new QCompiler();
    Set<String> regionsInQuery;
    try {
      CompiledValue compiledQuery = compiler.compileQuery(query);
      Set<String> regions = new HashSet<>();
      compiledQuery.getRegionsInQuery(regions, null);
      regionsInQuery = Collections.unmodifiableSet(regions);
      return regionsInQuery;
    } catch (QueryInvalidException qe) {
      logger.error("{} Failed, Error {}", query, qe.getMessage(), qe);
      throw qe;
    }
  }

  static class JsonisedErrorMessage {

    private final String message;

    public JsonisedErrorMessage(final String errorMessage) {
      message = errorMessage;
    }

    @Override
    public String toString() {
      return String.format("{\"message\":\"%s\"}", message);
    }
  }
}
