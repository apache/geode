/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

public class RegionSizeFunction implements Function, Declarable {

  private static final long serialVersionUID = -2791590491585777990L;
  
  public static final String ID = "region-size-function";

	public void execute(FunctionContext context) {
		RegionFunctionContext rfc = (RegionFunctionContext) context;
		context.getResultSender().lastResult(rfc.getDataSet().size());
	}

	public String getId() {
		return ID;
	}

	public boolean hasResult() {
		return true;
	}

	public boolean optimizeForWrite() {
		return true;
	}

	public boolean isHA() {
		return true;
	}

  @Override
  public void init(Properties arg0) {
  }
}
