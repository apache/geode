/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;


/**
 * 
 * Class for change log level function
 * 
 * @author apande
 * since 8.0 
 * 
 */

public class ChangeLogLevelFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = ChangeLogLevelFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Cache cache = CacheFactory.getAnyInstance();
    Map<String, String> result = new HashMap<String, String>();
    try{      
      LogWriterLogger logwriterLogger = (LogWriterLogger) cache.getLogger();      
      Object[] args = (Object[]) context.getArguments();      
      final String logLevel = (String) args[0];
      Level log4jLevel = LogWriterLogger.logWriterNametoLog4jLevel(logLevel);
      logwriterLogger.setLevel(log4jLevel);
      System.setProperty("gemfire.log-level", logLevel);
      // LOG:CONFIG:
      logger.info(LogMarker.CONFIG, "GFSH Changed log level to {}", log4jLevel);
      result.put(cache.getDistributedSystem().getDistributedMember().getId(), "New log level is " + log4jLevel);
      context.getResultSender().lastResult(result);
    }catch(Exception ex){      
      // LOG:CONFIG:
      logger.info(LogMarker.CONFIG, "GFSH Changing log level exception {}", ex.getMessage(), ex);
      result.put(cache.getDistributedSystem().getDistributedMember().getId(), "ChangeLogLevelFunction exception " + ex.getMessage());
      context.getResultSender().lastResult(result);
    }   
  }

  @Override
  public String getId() {
    return ChangeLogLevelFunction.ID;

  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}
