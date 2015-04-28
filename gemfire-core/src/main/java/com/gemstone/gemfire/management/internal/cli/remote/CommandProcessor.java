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
import java.util.Properties;

import org.springframework.shell.core.Parser;
import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.cli.CommandStatement;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.util.CommentSkipHelper;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class CommandProcessor {
  protected RemoteExecutionStrategy executionStrategy;
  protected Parser                  parser;  
  private   CommandManager          commandManager;
  private   int                     lastExecutionStatus;
  private   LogWrapper              logWrapper;
  
  // Lock to synchronize getters & stop
  private final Object LOCK = new Object();
  
  private volatile boolean isStopped = false;

  public CommandProcessor() throws ClassNotFoundException, IOException {
    this(null);
  }
  
  public CommandProcessor(Properties cacheProperties) throws ClassNotFoundException, IOException {
    this.commandManager    = CommandManager.getInstance(cacheProperties);
    this.executionStrategy = new RemoteExecutionStrategy();
    this.parser            = new GfshParser(commandManager);
    this.logWrapper        = LogWrapper.getInstance();
  }
  
  protected RemoteExecutionStrategy getExecutionStrategy() {
    synchronized (LOCK) {
      return executionStrategy;
    }
  }

  protected Parser getParser() {
    synchronized (LOCK) {
      return parser;
    }
  }

////stripped down AbstractShell.executeCommand
  public ParseResult parseCommand(String commentLessLine) throws CommandProcessingException, IllegalStateException {
    if (commentLessLine != null) {
      return getParser().parse(commentLessLine);
    }
    throw new IllegalStateException("Command String should not be null.");
  }
  
  public Result executeCommand(CommandStatement cmdStmt) {
    Object result        = null;
    Result commandResult = null;

    CommentSkipHelper commentSkipper = new CommentSkipHelper();
    String commentLessLine = commentSkipper.skipComments(cmdStmt.getCommandString());
    if (commentLessLine != null && !commentLessLine.isEmpty()) {
      CommandExecutionContext.setShellEnv(cmdStmt.getEnv());

      final RemoteExecutionStrategy executionStrategy = getExecutionStrategy();
      try {
        ParseResult parseResult = ((CommandStatementImpl)cmdStmt).getParseResult();
        
        if (parseResult == null) {
          parseResult = parseCommand(commentLessLine);
          if (parseResult == null) {//TODO-Abhishek: Handle this in GfshParser Implementation
            setLastExecutionStatus(1);
            return ResultBuilder.createParsingErrorResult(cmdStmt.getCommandString());
          }
          ((CommandStatementImpl)cmdStmt).setParseResult(parseResult);
        }
        result = executionStrategy.execute(parseResult);
        if (result instanceof Result) {
          commandResult = (Result) result;
        } else {
          if (logWrapper.fineEnabled()) {
            logWrapper.fine("Unknown result type, using toString : "+String.valueOf(result));
          }
          commandResult = ResultBuilder.createInfoResult(String.valueOf(result));
        }
      } catch (CommandProcessingException e) { //expected from Parser
        setLastExecutionStatus(1);
        if (logWrapper.infoEnabled()) {
          logWrapper.info("Could not parse \""+cmdStmt.getCommandString()+"\".", e);
        }
        return ResultBuilder.createParsingErrorResult(e.getMessage());
      } catch (RuntimeException e) {
        setLastExecutionStatus(1);
        if (logWrapper.infoEnabled()) {
          logWrapper.info("Could not execute \""+cmdStmt.getCommandString()+"\".", e);
        }
        return ResultBuilder.createGemFireErrorResult("Error while processing command <" +cmdStmt.getCommandString()+"> Reason : " + e.getMessage());
      } catch (Exception e) {
        setLastExecutionStatus(1);
        if (logWrapper.warningEnabled()) {
          logWrapper.warning("Could not execute \""+cmdStmt.getCommandString()+"\".", e);
        }
        return ResultBuilder.createGemFireErrorResult("Unexpected error while processing command <" +cmdStmt.getCommandString()+"> Reason : " + e.getMessage());
      }
      if (logWrapper.fineEnabled()) {
        logWrapper.fine("Executed " + commentLessLine);
      }
      setLastExecutionStatus(0);
    }

    return commandResult;
  }

  public CommandStatement createCommandStatement(String commandString, Map<String, String> env) {
    return new CommandStatementImpl(commandString, env, this);
  }

  public int getLastExecutionStatus() {
    return lastExecutionStatus;
  }

  public void setLastExecutionStatus(int lastExecutionStatus) {
    this.lastExecutionStatus = lastExecutionStatus;
  }
  
  public boolean isStopped() {
    return isStopped;
  }

  public void stop() {
    synchronized (LOCK) {
      this.commandManager    = null;
      this.executionStrategy = null;
      this.parser            = null;
      this.isStopped         = true;
    }
  }
}
