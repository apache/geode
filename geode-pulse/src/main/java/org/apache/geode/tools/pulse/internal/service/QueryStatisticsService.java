/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.service;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class QueryStatisticsService
 *
 * This class returns top N queries based on pagination and filtering criteria if any
 *
 * @since GemFire version 7.5
 */
@Component
@Service("QueryStatistics")
@Scope("singleton")
public class QueryStatisticsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    Cluster.Statement[] stmts = cluster.getStatements();

    ArrayNode queryListJson = mapper.createArrayNode();
    for (Cluster.Statement stmt : stmts) {
      ObjectNode queryJSON = mapper.createObjectNode();
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QUERYDEFINITION, stmt.getQueryDefinition());
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED, mapper.valueToTree(
          stmt.getNumTimesCompiled() < 0 ? VALUE_NA : stmt.getNumTimesCompiled()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION, mapper.valueToTree(
          stmt.getNumExecution() < 0 ? VALUE_NA : stmt.getNumExecution()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
          mapper.valueToTree(stmt.getNumExecutionsInProgress() < 0 ? VALUE_NA
              : stmt.getNumExecutionsInProgress()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
          mapper.valueToTree(stmt.getNumTimesGlobalIndexLookup() < 0 ? VALUE_NA
              : stmt.getNumTimesGlobalIndexLookup()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED, mapper.valueToTree(
          stmt.getNumRowsModified() < 0 ? VALUE_NA : stmt.getNumRowsModified()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_PARSETIME, mapper
          .valueToTree(stmt.getParseTime() < 0 ? VALUE_NA : stmt.getParseTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_BINDTIME,
          mapper.valueToTree(stmt.getBindTime() < 0 ? VALUE_NA : stmt.getBindTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME, mapper.valueToTree(
          stmt.getOptimizeTime() < 0 ? VALUE_NA : stmt.getOptimizeTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME, mapper.valueToTree(
          stmt.getRoutingInfoTime() < 0 ? VALUE_NA : stmt.getRoutingInfoTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME, mapper.valueToTree(
          stmt.getGenerateTime() < 1 ? VALUE_NA : stmt.getGenerateTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME,
          mapper.valueToTree(stmt.getTotalCompilationTime() < 0 ? VALUE_NA
              : stmt.getTotalCompilationTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME, mapper.valueToTree(
          stmt.getExecutionTime() < 0 ? VALUE_NA : stmt.getExecutionTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME, mapper.valueToTree(
          stmt.getProjectionTime() < 0 ? VALUE_NA : stmt.getProjectionTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME, mapper.valueToTree(
          stmt.getTotalExecutionTime() < 0 ? VALUE_NA : stmt.getTotalExecutionTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME,
          mapper.valueToTree(stmt.getRowsModificationTime() < 0 ? VALUE_NA
              : stmt.getRowsModificationTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN, mapper.valueToTree(
          stmt.getqNNumRowsSeen() < 0 ? VALUE_NA : stmt.getqNNumRowsSeen()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME, mapper.valueToTree(
          stmt.getqNMsgSendTime() < 0 ? VALUE_NA : stmt.getqNMsgSendTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME, mapper.valueToTree(
          stmt.getqNMsgSerTime() < 0 ? VALUE_NA : stmt.getqNMsgSerTime()));
      queryJSON.set(PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME, mapper.valueToTree(
          stmt.getqNRespDeSerTime() < 0 ? VALUE_NA : stmt.getqNRespDeSerTime()));
      queryListJson.add(queryJSON);
    }
    responseJSON.set("queriesList", queryListJson);

    // return jmx status
    responseJSON.put("connectedFlag", cluster.isConnectedFlag());
    responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

    // Send json response
    return responseJSON;

  }
}
