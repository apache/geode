/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.tools.gfsh.aggregator;

import java.util.Properties;

//import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;

/**
 * The Aggregator partition function. Used internally.
 * 
 * @author dpark
 */
public class AggregatorPartitionFunction implements /*Declarable, */Function, InternalEntity {
  private static final long serialVersionUID = 1L;

  public final static String ID = "__gfsh_aggregator";

  public AggregatorPartitionFunction() {
  }

  public String getId() {
    return ID;
  }

  public void execute(FunctionContext context) {
    AggregateFunction aggregator = (AggregateFunction) context.getArguments();
    context.getResultSender().lastResult(aggregator.run(context));
  }

  public void init(Properties p) {
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }
}
