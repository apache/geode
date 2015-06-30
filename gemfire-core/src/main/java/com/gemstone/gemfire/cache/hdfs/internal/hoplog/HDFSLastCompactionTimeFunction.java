/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Function that returns the oldest timestamp among all the major
 * compacted buckets on the members
 *
 * @author sbawaska
 */
@SuppressWarnings("serial")
public class HDFSLastCompactionTimeFunction extends FunctionAdapter implements InternalEntity{

  public static final String ID = "HDFSLastCompactionTimeFunction";

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    PartitionedRegion pr = (PartitionedRegion) rfc.getDataSet();
    rfc.getResultSender().lastResult(pr.lastLocalMajorHDFSCompaction());
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
