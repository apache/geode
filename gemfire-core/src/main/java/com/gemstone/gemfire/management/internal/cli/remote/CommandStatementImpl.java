/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.remote;

import java.util.Collections;
import java.util.Map;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CommandStatement;
import com.gemstone.gemfire.management.cli.Result;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class CommandStatementImpl implements CommandStatement {
  
  private CommandProcessor cmdProcessor;
  private String commandString;
  private Map<String, String> env;
  private ParseResult parseResult;

  CommandStatementImpl(String commandString, Map<String, String> env, CommandProcessor cmdProcessor) {
    this.commandString = commandString;
    this.env           = env;
    this.cmdProcessor  = cmdProcessor;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#getCommandString()
   */
  @Override
  public String getCommandString() {
    return commandString;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#getEnv()
   */
  @Override
  public Map<String, String> getEnv() {
    return env;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#process()
   */
  @Override
  public Result process() {
    return cmdProcessor.executeCommand(this);
  }

  /**
   * @return the parseResult
   */
  ParseResult getParseResult() {
    return parseResult;
  }

  /**
   * @param parseResult the parseResult to set
   */
  void setParseResult(ParseResult parseResult) {
    this.parseResult = parseResult;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#validate()
   */
  @Override
  public boolean validate() {
    //TODO-Abhishek: is not null check enough?
    return parseResult != null;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return CommandStatement.class.getSimpleName() + "[commandString=" + commandString + ", env=" + env + "]";
  }
}
