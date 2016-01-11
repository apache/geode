/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public interface AggregateStatementMBean {
  String OBJECT_NAME = "GemFireXD:service=Statement,type=Aggregate";

  /**
   * Query definition
   *
   * @return
   */
  String getQueryDefinition();

  /**
   * Number of times this statement is compiled (including re compilations)
   *
   * @return
   */
  long getNumTimesCompiled();

  /**
   * Number of times this statement is executed
   *
   * @return
   */
  long getNumExecution();

  /**
   * Statements that are actively being processed during the statistics snapshot
   *
   * @return
   */
  long getNumExecutionsInProgress();

  /**
   * Number of times global index lookup message exchanges occurred
   *
   * @return
   */
  long getNumTimesGlobalIndexLookup();

  /**
   * Number of rows modified by DML operation of insert/delete/update
   *
   * @return
   */
  long getNumRowsModified();

  /**
   * Time spent(in milliseconds) in parsing the query string
   *
   * @return
   */
  long getParseTime();

  /**
   * Time spent (in milliseconds) mapping this statement with database object's metadata (bind)
   *
   * @return
   */
  long getBindTime();

  /**
   * Time spent (in milliseconds) determining the best execution path for this statement
   * (optimize)
   *
   * @return
   */
  long getOptimizeTime();

  /**
   * Time spent (in milliseconds) compiling details about routing information of query strings to
   * data node(s) (processQueryInfo)
   *
   * @return
   */
  long getRoutingInfoTime();

  /**
   * Time spent (in milliseconds) to generate query execution plan definition (activation class)
   *
   * @return
   */
  long getGenerateTime();

  /**
   * Total compilation time (in milliseconds) of the statement on this node (prepMinion)
   *
   * @return
   */
  long getTotalCompilationTime();

  /**
   * Time spent (in nanoseconds) in creation of all the layers of query processing (ac.execute)
   *
   * @return
   */
  long getExecutionTime();

  /**
   * Time to apply (in nanoseconds) the projection and additional filters. (projectrestrict)
   *
   * @return
   */
  long getProjectionTime();

  /**
   * Total execution time (in nanoseconds) taken to process the statement on this node
   * (execute/open/next/close)
   *
   * @return
   */
  long getTotalExecutionTime();

  /**
   * Time taken (in nanoseconds) to modify rows by DML operation of insert/delete/update
   *
   * @return
   */
  long getRowsModificationTime();

  /**
   * Number of rows returned from remote nodes (ResultHolder/Get convertibles)
   *
   * @return
   */
  long getQNNumRowsSeen();

  /**
   * TCP send time (in nanoseconds) of all the messages including serialization time and queue
   * wait time
   *
   * @return
   */
  long getQNMsgSendTime();

  /**
   * Serialization time (in nanoseconds) for all the messages while sending to remote node(s)
   *
   * @return
   */
  long getQNMsgSerTime();

  /**
   *
   *
   * @return
   */
  long getQNRespDeSerTime();

}
