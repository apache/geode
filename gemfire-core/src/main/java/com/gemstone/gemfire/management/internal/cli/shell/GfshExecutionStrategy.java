/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell;

import static com.gemstone.gemfire.management.internal.cli.multistep.CLIMultiStepHelper.execCLISteps;

import java.lang.reflect.Method;
import java.util.Map;

import org.springframework.shell.core.ExecutionStrategy;
import org.springframework.shell.core.Shell;
import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CommandRequest;
import com.gemstone.gemfire.management.internal.cli.CommandResponse;
import com.gemstone.gemfire.management.internal.cli.CommandResponseBuilder;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.multistep.MultiStepCommand;
import com.gemstone.gemfire.management.internal.cli.result.FileResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.util.spring.Assert;
import com.gemstone.gemfire.management.internal.cli.util.spring.ReflectionUtils;

/**
 * Defines the {@link ExecutionStrategy} for commands that are executed in
 * GemFire SHell (gfsh).
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class GfshExecutionStrategy implements ExecutionStrategy {
  private Class<?>   mutex = GfshExecutionStrategy.class;
  private Gfsh       shell;
  private LogWrapper logWrapper;
  
  GfshExecutionStrategy(Gfsh shell) {
    this.shell      = shell;
    this.logWrapper = LogWrapper.getInstance();
  }

  //////////////// ExecutionStrategy interface Methods Start ///////////////////
  ///////////////////////// Implemented Methods ////////////////////////////////
  /**
   * Executes the method indicated by the {@link ParseResult} which would always
   * be {@link GfshParseResult} for GemFire defined commands. If the command
   * Method is decorated with {@link CliMetaData#shellOnly()} set to
   * <code>false</code>, {@link OperationInvoker} is used to send the command
   * for processing on a remote GemFire node.
   * 
   * @param parseResult
   *          that should be executed (never presented as null)
   * @return an object which will be rendered by the {@link Shell}
   *         implementation (may return null)
   * @throws RuntimeException
   *           which is handled by the {@link Shell} implementation
   */
  @Override
  public Object execute(ParseResult parseResult){
    Object result = null;
    Method method = parseResult.getMethod();
    try {
      
    //Check if it's a multi-step command
      Method reflectmethod = parseResult.getMethod();
      MultiStepCommand cmd = reflectmethod.getAnnotation(MultiStepCommand.class);      
      if(cmd!=null){
        return execCLISteps(logWrapper, shell,parseResult);
      }

//      See #46072 
//      String commandName = getCommandName(parseResult);
//      if (commandName != null) {
//        shell.flashMessage("Executing " + getCommandName(parseResult) + " ... ");
//      }
      //Check if it's a remote command
      if (!isShellOnly(method)) {
        if (GfshParseResult.class.isInstance(parseResult)) {
          result = executeOnRemote((GfshParseResult)parseResult);
        } else {//Remote command means implemented for Gfsh and ParseResult should be GfshParseResult.
          //TODO - Abhishek: should this message be more specific?
          throw new IllegalStateException("Configuration error!");
        }
      } else {
        Assert.notNull(parseResult, "Parse result required");
        synchronized (mutex) {
          //TODO: Remove Assert
          Assert.isTrue(isReadyForCommands(), "ProcessManagerHostedExecutionStrategy not yet ready for commands");
          result = ReflectionUtils.invokeMethod(parseResult.getMethod(), parseResult.getInstance(), parseResult.getArguments());
        }
      }
//    See #46072
//      shell.flashMessage("");
    } catch (JMXInvocationException e) {
      Gfsh.getCurrentInstance().logWarning(e.getMessage(), e);
    } catch (IllegalStateException e) {
      // Shouldn't occur - we are always using GfsParseResult
      Gfsh.getCurrentInstance().logWarning(e.getMessage(), e);
    } catch (CommandProcessingException e) {
      Gfsh.getCurrentInstance().logWarning(e.getMessage(), null);
      Object errorData = e.getErrorData();
      if (errorData != null && errorData instanceof Throwable) {
        logWrapper.warning(e.getMessage(), (Throwable) errorData);
      } else {
        logWrapper.warning(e.getMessage());
      }
    } catch (RuntimeException e) {
      Gfsh.getCurrentInstance().logWarning("Exception occurred. " + e.getMessage(), e);
      // Log other runtime exception in gfsh log
      logWrapper.warning("Error occurred while executing command : "+((GfshParseResult)parseResult).getUserInput(), e);
    } catch (Exception e) {
      Gfsh.getCurrentInstance().logWarning("Unexpected exception occurred. " + e.getMessage(), e);
      // Log other exceptions in gfsh log
      logWrapper.warning("Unexpected error occurred while executing command : "+((GfshParseResult)parseResult).getUserInput(), e);
    }
    
    return result;
  }
  
  /**
   * Whether the command is available only at the shell or on GemFire member
   * too.
   * 
   * @param method
   *          the method to check the associated annotation
   * @return true if CliMetaData is added to the method & CliMetaData.shellOnly
   *         is set to true, false otherwise
   */
  private boolean isShellOnly(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null && cliMetadata.shellOnly();
  }

  private String getInterceptor(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null ? cliMetadata.interceptor() : CliMetaData.ANNOTATION_NULL_VALUE;
  }

//  Not used currently
//  private static String getCommandName(ParseResult result) {
//    Method method = result.getMethod();
//    CliCommand cliCommand = method.getAnnotation(CliCommand.class);
//
//    return cliCommand != null ? cliCommand.value() [0] : null;
//  }

  /**
   * Indicates commands are able to be presented. This generally means all 
   * important system startup activities have completed.
   * Copied from {@link ExecutionStrategy#isReadyForCommands()}.
   *
   * @return whether commands can be presented for processing at this time
   */
  @Override
  public boolean isReadyForCommands() {
    return true;
  }

  /**
   * Indicates the execution runtime should be terminated. This allows it to 
   * cleanup before returning control flow to the caller. Necessary for clean 
   * shutdowns.
   * Copied from {@link ExecutionStrategy#terminate()}.
   */
  @Override
  public void terminate() {
    //TODO: Is additional cleanup required?
    shell = null;
  }
  //////////////// ExecutionStrategy interface Methods End /////////////////////
  
  /**
   * Sends the user input (command string) via {@link OperationInvoker} to a
   * remote GemFire node for processing & execution.
   *
   * @param parseResult 
   * 
   * @return result of execution/processing of the command
   * 
   * @throws IllegalStateException
   *           if gfsh doesn't have an active connection.
   */
  private Result executeOnRemote(GfshParseResult parseResult) {
    Result   commandResult = null;
    Object   response      = null;
    if (shell.isConnectedAndReady()) {
      byte[][]             fileData    = null;
      CliAroundInterceptor interceptor = null;
      
      String interceptorClass = getInterceptor(parseResult.getMethod());
      
      //1. Pre Remote Execution
      if (!CliMetaData.ANNOTATION_NULL_VALUE.equals(interceptorClass)) {
        try {
          interceptor = (CliAroundInterceptor) ClassPathLoader.getLatest().forName(interceptorClass).newInstance();
        } catch (InstantiationException e) {
          shell.logWarning("Configuration error", e);
        } catch (IllegalAccessException e) {
          shell.logWarning("Configuration error", e);
        } catch (ClassNotFoundException e) {
          shell.logWarning("Configuration error", e);
        }
        if (interceptor != null) {
          Result preExecResult = interceptor.preExecution(parseResult);
          if (Status.ERROR.equals(preExecResult.getStatus())) {
            return preExecResult;
          } else if (preExecResult instanceof FileResult) {            
            FileResult fileResult = (FileResult) preExecResult;
            fileData = fileResult.toBytes();
          }
        } else {
          return ResultBuilder.createBadConfigurationErrorResult("Interceptor Configuration Error");
        }
      }

      //2. Remote Execution
      final Map<String, String> env = shell.getEnv();
      response = shell.getOperationInvoker().processCommand(new CommandRequest(parseResult, env, fileData));
      env.clear();
      
      if (response == null) {
        shell.logWarning("Response was null for: \""+parseResult.getUserInput()+"\". (gfsh.isConnected="+shell.isConnectedAndReady()+")", null);
        commandResult = 
            ResultBuilder.createBadResponseErrorResult(" Error occurred while " + 
                "executing \""+parseResult.getUserInput()+"\" on manager. " +
                		"Please check manager logs for error.");
      } else {
        if (logWrapper.fineEnabled()) {
          logWrapper.fine("Received response :: "+response);
        }
        CommandResponse commandResponse = CommandResponseBuilder.prepareCommandResponseFromJson((String) response);
        
        if (commandResponse.isFailedToPersist()) {
          shell.printAsSevere(CliStrings.SHARED_CONFIGURATION_FAILED_TO_PERSIST_COMMAND_CHANGES);
          logWrapper.severe(CliStrings.SHARED_CONFIGURATION_FAILED_TO_PERSIST_COMMAND_CHANGES);
        }
        
        String debugInfo = commandResponse.getDebugInfo();
        if (debugInfo != null && !debugInfo.trim().isEmpty()) {
          //TODO - Abhishek When debug is ON, log response in gfsh logs
          //TODO - Abhishek handle \n better. Is it coming from GemFire formatter
          debugInfo = debugInfo.replaceAll("\n\n\n", "\n");
          debugInfo = debugInfo.replaceAll("\n\n", "\n");
          debugInfo = debugInfo.replaceAll("\n", "\n[From Manager : "+commandResponse.getSender()+"]");
          debugInfo = "[From Manager : "+commandResponse.getSender()+"]" + debugInfo;
          LogWrapper.getInstance().info(debugInfo);
        }
        commandResult = ResultBuilder.fromJson((String) response);
        
        
        //3. Post Remote Execution
        if (interceptor != null) {
          Result postExecResult = interceptor.postExecution(parseResult, commandResult);
          if (postExecResult != null) {
            if (Status.ERROR.equals(postExecResult.getStatus())) {
              if (logWrapper.infoEnabled()) {
                logWrapper.info("Post execution Result :: "+ResultBuilder.resultAsString(postExecResult));
              }
            } else if (logWrapper.fineEnabled()) {
              logWrapper.fine("Post execution Result :: "+ResultBuilder.resultAsString(postExecResult));
            }
            commandResult = postExecResult;
          }
        }
      }// not null response
    } else {
      shell.logWarning("Can't execute a remote command without connection. Use 'connect' first to connect.", null);
      logWrapper.info("Can't execute a remote command \""+parseResult.getUserInput()+"\" without connection. Use 'connect' first to connect to GemFire.");
    }
    return commandResult;
  }
}
