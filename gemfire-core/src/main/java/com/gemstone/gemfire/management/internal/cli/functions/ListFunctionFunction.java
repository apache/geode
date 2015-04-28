/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;

public class ListFunctionFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  public static final String ID = ListFunctionFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    
    try {
      final Object[] args = (Object[]) context.getArguments();
      final String stringPattern = (String) args[0];

      Cache cache = CacheFactory.getAnyInstance();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      final Map<String, Function> functions = FunctionService.getRegisteredFunctions();
      CliFunctionResult result;
      if (stringPattern == null || stringPattern.isEmpty()) {
        result = new CliFunctionResult(memberId, functions.keySet().toArray(new String[0]));
      } else {
        Pattern pattern = Pattern.compile(stringPattern);
        List<String> resultList = new LinkedList<String>();
        for (String functionId : functions.keySet()) {
          Matcher matcher = pattern.matcher(functionId);
          if (matcher.matches()) {
            resultList.add(functionId);
          }
        }
        result = new CliFunctionResult(memberId, resultList.toArray(new String[0]));
      }
      context.getResultSender().lastResult(result);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not list functions: {}", th.getMessage(), th);
      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
