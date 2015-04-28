/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.remote;

import java.io.IOException;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.management.cli.CommandService;
import com.gemstone.gemfire.management.cli.CommandServiceException;
import com.gemstone.gemfire.management.cli.CommandStatement;
import com.gemstone.gemfire.management.cli.Result;

/**
 * @author Abhishek Chaudhari
 */
public class MemberCommandService extends CommandService {
  private final Object modLock = new Object();
  
  private Cache            cache;
  private CommandProcessor commandProcessor;

  public MemberCommandService(Cache cache) throws CommandServiceException {
    this.cache = cache;
    try {
      this.commandProcessor = new CommandProcessor(cache.getDistributedSystem().getProperties());
    } catch (ClassNotFoundException e) {
      throw new CommandServiceException("Could not load commands.", e);
    } catch (IOException e) {
      throw new CommandServiceException("Could not load commands.", e);
    } catch (IllegalStateException e) {
      throw new CommandServiceException(e.getMessage(), e);
    }
  }
  
  public Result processCommand(String commandString) {
    return this.processCommand(commandString, EMPTY_ENV);
  }
  
  public Result processCommand(String commandString, Map<String, String> env) {
    return createCommandStatement(commandString, env).process();
  }
  
  public CommandStatement createCommandStatement(String commandString) {
    return this.createCommandStatement(commandString, EMPTY_ENV);
  }
  
  public CommandStatement createCommandStatement(String commandString, Map<String, String> env) {
    if (!isUsable()) {
      throw new IllegalStateException("Cache instance is not available.");
    }
    synchronized (modLock) {
      return commandProcessor.createCommandStatement(commandString, env);
    }
  }

  @Override
  public boolean isUsable() {
    return (this.cache != null && !this.cache.isClosed());
  }

//  @Override
//  public void stop() {
//    cache = null;
//    synchronized (modLock) {
//      this.commandProcessor.stop();
//      this.commandProcessor = null;
//    }
//  }  
}
