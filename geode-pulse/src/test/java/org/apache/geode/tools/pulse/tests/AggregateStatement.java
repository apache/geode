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
package org.apache.geode.tools.pulse.tests;

public class AggregateStatement extends JMXBaseBean implements AggregateStatementMBean {
  private String name = null;

  public AggregateStatement(String name) {
    this.name = name;
  }

  protected String getKey(String propName) {
    return "aggregatestatement." + name + "." + propName;
  }
  
  /**
   * Query definition
   * 
   * @return
   */
  public String getQueryDefinition(){
    return getString("queryDefinition");
  }

  /**
   * Number of times this statement is compiled (including re compilations)
   * 
   * @return
   */
  @Override
  public long getNumTimesCompiled(){
    return getLong("numTimesCompiled");
  }

  /**
   * Number of times this statement is executed
   * 
   * @return
   */
  @Override
  public long getNumExecution(){
    return getLong("numExecution");
  }

  /**
   * Statements that are actively being processed during the statistics snapshot
   * 
   * @return
   */
  public long getNumExecutionsInProgress(){
    return getLong("numExecutionsInProgress");
  }

  /**
   * Number of times global index lookup message exchanges occurred
   * 
   * @return
   */
  public long getNumTimesGlobalIndexLookup(){
    return getLong("numTimesGlobalIndexLookup");
  }

  /**
   * Number of rows modified by DML operation of insert/delete/update
   * 
   * @return
   */
  public long getNumRowsModified(){
    return getLong("numRowsModified");
  }

  /**
   * Time spent(in milliseconds) in parsing the query string
   * 
   * @return
   */
  public long getParseTime(){
    return getLong("parseTime");
  }

  /**
   * Time spent (in milliseconds) mapping this statement with database object's metadata (bind)
   * 
   * @return
   */
  public long getBindTime(){
    return getLong("bindTime");
  }

  /**
   * Time spent (in milliseconds) determining the best execution path for this statement
   * (optimize)
   * 
   * @return
   */
  public long getOptimizeTime(){
    return getLong("optimizeTime");
  }

  /**
   * Time spent (in milliseconds) compiling details about routing information of query strings to
   * data node(s) (processQueryInfo)
   * 
   * @return
   */
  public long getRoutingInfoTime(){
    return getLong("routingInfoTime");
  }

  /**
   * Time spent (in milliseconds) to generate query execution plan definition (activation class)
   * 
   * @return
   */
  public long getGenerateTime(){
    return getLong("generateTime");
  }

  /**
   * Total compilation time (in milliseconds) of the statement on this node (prepMinion)
   * 
   * @return
   */
  public long getTotalCompilationTime(){
    return getLong("totalCompilationTime");
  }

  /**
   * Time spent (in nanoseconds) in creation of all the layers of query processing (ac.execute)
   * 
   * @return
   */
  public long getExecutionTime(){
    return getLong("executionTime");
  }

  /**
   * Time to apply (in nanoseconds) the projection and additional filters. (projectrestrict)
   * 
   * @return
   */
  public long getProjectionTime(){
    return getLong("projectionTime");
  }

  /**
   * Total execution time (in nanoseconds) taken to process the statement on this node
   * (execute/open/next/close)
   * 
   * @return
   */
  public long getTotalExecutionTime(){
    return getLong("totalExecutionTime");
  }

  /**
   * Time taken (in nanoseconds) to modify rows by DML operation of insert/delete/update
   * 
   * @return
   */
  public long getRowsModificationTime(){
    return getLong("rowsModificationTime");
  }

  /**
   * Number of rows returned from remote nodes (ResultHolder/Get convertibles)
   * 
   * @return
   */
  public long getQNNumRowsSeen(){
    return getLong("qnNumRowsSeen");
  }

  /**
   * TCP send time (in nanoseconds) of all the messages including serialization time and queue
   * wait time
   * 
   * @return
   */
  public long getQNMsgSendTime(){
    return getLong("qnMsgSendTime");
  }

  /**
   * Serialization time (in nanoseconds) for all the messages while sending to remote node(s)
   * 
   * @return
   */
  public long getQNMsgSerTime(){
    return getLong("qnMsgSerTime");
  }
  
  /**
   * 
   * 
   * @return
   */
  public long getQNRespDeSerTime(){
    return getLong("qnRespDeSerTime");
  }
}
