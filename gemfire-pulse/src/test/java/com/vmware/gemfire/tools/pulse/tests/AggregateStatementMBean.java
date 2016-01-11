/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public interface AggregateStatementMBean {
  public static final String OBJECT_NAME = "GemFireXD:service=Statement,type=Aggregate";

  /**
   * Query definition
   *
   * @return
   */
  public String getQueryDefinition();

  /**
   * Number of times this statement is compiled (including re compilations)
   *
   * @return
   */
  public long getNumTimesCompiled();

  /**
   * Number of times this statement is executed
   *
   * @return
   */
  public long getNumExecution();

  /**
   * Statements that are actively being processed during the statistics snapshot
   *
   * @return
   */
  public long getNumExecutionsInProgress();

  /**
   * Number of times global index lookup message exchanges occurred
   *
   * @return
   */
  public long getNumTimesGlobalIndexLookup();

  /**
   * Number of rows modified by DML operation of insert/delete/update
   *
   * @return
   */
  public long getNumRowsModified();

  /**
   * Time spent(in milliseconds) in parsing the query string
   *
   * @return
   */
  public long getParseTime();

  /**
   * Time spent (in milliseconds) mapping this statement with database object's metadata (bind)
   *
   * @return
   */
  public long getBindTime();

  /**
   * Time spent (in milliseconds) determining the best execution path for this statement
   * (optimize)
   *
   * @return
   */
  public long getOptimizeTime();

  /**
   * Time spent (in milliseconds) compiling details about routing information of query strings to
   * data node(s) (processQueryInfo)
   *
   * @return
   */
  public long getRoutingInfoTime();

  /**
   * Time spent (in milliseconds) to generate query execution plan definition (activation class)
   *
   * @return
   */
  public long getGenerateTime();

  /**
   * Total compilation time (in milliseconds) of the statement on this node (prepMinion)
   *
   * @return
   */
  public long getTotalCompilationTime();

  /**
   * Time spent (in nanoseconds) in creation of all the layers of query processing (ac.execute)
   *
   * @return
   */
  public long getExecutionTime();

  /**
   * Time to apply (in nanoseconds) the projection and additional filters. (projectrestrict)
   *
   * @return
   */
  public long getProjectionTime();

  /**
   * Total execution time (in nanoseconds) taken to process the statement on this node
   * (execute/open/next/close)
   *
   * @return
   */
  public long getTotalExecutionTime();

  /**
   * Time taken (in nanoseconds) to modify rows by DML operation of insert/delete/update
   *
   * @return
   */
  public long getRowsModificationTime();

  /**
   * Number of rows returned from remote nodes (ResultHolder/Get convertibles)
   *
   * @return
   */
  public long getQNNumRowsSeen();

  /**
   * TCP send time (in nanoseconds) of all the messages including serialization time and queue
   * wait time
   *
   * @return
   */
  public long getQNMsgSendTime();

  /**
   * Serialization time (in nanoseconds) for all the messages while sending to remote node(s)
   *
   * @return
   */
  public long getQNMsgSerTime();

  /**
   *
   *
   * @return
   */
  public long getQNRespDeSerTime();

}
