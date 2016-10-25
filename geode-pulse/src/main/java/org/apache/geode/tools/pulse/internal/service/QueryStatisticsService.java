/*
 *
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
 * This class returns top N queries based on pagination and filtering criteria
 * if any
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
    for (int i = 0; i < stmts.length; ++i) {
      ObjectNode queryJSON = mapper.createObjectNode();
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QUERYDEFINITION, stmts[i].getQueryDefinition());
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
          mapper.valueToTree(stmts[i].getNumTimesCompiled() < 0 ? this.VALUE_NA : stmts[i].getNumTimesCompiled()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION,
          mapper.valueToTree(stmts[i].getNumExecution() < 0 ? this.VALUE_NA : stmts[i].getNumExecution()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
          mapper.valueToTree(stmts[i].getNumExecutionsInProgress() < 0 ? this.VALUE_NA : stmts[i].getNumExecutionsInProgress()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
          mapper.valueToTree(stmts[i].getNumTimesGlobalIndexLookup() < 0 ? this.VALUE_NA : stmts[i].getNumTimesGlobalIndexLookup()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED,
          mapper.valueToTree(stmts[i].getNumRowsModified() < 0 ? this.VALUE_NA : stmts[i].getNumRowsModified()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_PARSETIME,
          mapper.valueToTree(stmts[i].getParseTime() < 0 ? this.VALUE_NA : stmts[i].getParseTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_BINDTIME,
          mapper.valueToTree(stmts[i].getBindTime() < 0 ? this.VALUE_NA : stmts[i].getBindTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME,
          mapper.valueToTree(stmts[i].getOptimizeTime() < 0 ? this.VALUE_NA : stmts[i].getOptimizeTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME,
          mapper.valueToTree(stmts[i].getRoutingInfoTime() < 0 ? this.VALUE_NA : stmts[i].getRoutingInfoTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME,
          mapper.valueToTree(stmts[i].getGenerateTime() < 1 ? this.VALUE_NA : stmts[i].getGenerateTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME,
          mapper.valueToTree(stmts[i].getTotalCompilationTime() < 0 ? this.VALUE_NA : stmts[i].getTotalCompilationTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME,
          mapper.valueToTree(stmts[i].getExecutionTime() < 0 ? this.VALUE_NA : stmts[i].getExecutionTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME,
          mapper.valueToTree(stmts[i].getProjectionTime() < 0 ? this.VALUE_NA : stmts[i].getProjectionTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME,
          mapper.valueToTree(stmts[i].getTotalExecutionTime() < 0 ? this.VALUE_NA : stmts[i].getTotalExecutionTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME,
          mapper.valueToTree(stmts[i].getRowsModificationTime() < 0 ? this.VALUE_NA : stmts[i].getRowsModificationTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
          mapper.valueToTree(stmts[i].getqNNumRowsSeen() < 0 ? this.VALUE_NA : stmts[i].getqNNumRowsSeen()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME,
          mapper.valueToTree(stmts[i].getqNMsgSendTime() < 0 ? this.VALUE_NA : stmts[i].getqNMsgSendTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME,
          mapper.valueToTree(stmts[i].getqNMsgSerTime() < 0 ? this.VALUE_NA : stmts[i].getqNMsgSerTime()));
      queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME,
          mapper.valueToTree(stmts[i].getqNRespDeSerTime() < 0 ? this.VALUE_NA : stmts[i].getqNRespDeSerTime()));
      queryListJson.add(queryJSON);
    }
    responseJSON.put("queriesList", queryListJson);

    // return jmx status
    responseJSON.put("connectedFlag", cluster.isConnectedFlag());
    responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

    // Send json response
    return responseJSON;

  }
}
