/*
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
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


/**
 * 
 * Class for change log level function
 * 
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
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + LOG_LEVEL, logLevel);
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
