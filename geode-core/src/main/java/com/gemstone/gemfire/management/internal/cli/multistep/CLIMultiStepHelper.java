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
package com.gemstone.gemfire.management.internal.cli.multistep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.CommandRequest;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

import org.springframework.shell.event.ParseResult;
import org.springframework.util.ReflectionUtils;

/**
 * Utility class to abstract CompositeResultData for Multi-step commands
 * Also contain execution strategy for multi-step commands
 * 
 * 
 */
public class CLIMultiStepHelper {

  public static final String STEP_SECTION = "STEP_SECTION";
  public static final String PAGE_SECTION = "PAGE_SECTION";
  public static final String ARG_SECTION = "ARG_SECTION";
  public static final String NEXT_STEP_NAME = "NEXT_STEP_NAME";
  public static final String NEXT_STEP_ARGS = "NEXT_STEP_ARGS";
  public static final String NEXT_STEP_NAMES = "NEXT_STEP_NAMES";
  public static final String STEP_ARGS = "stepArgs";
  public static final int DEFAULT_PAGE_SIZE = 20;

  public static Object execCLISteps(LogWrapper logWrapper, Gfsh shell, ParseResult parseResult) {
    CLIStep[] steps = (CLIStep[]) ReflectionUtils.invokeMethod(parseResult.getMethod(), parseResult.getInstance(),
        parseResult.getArguments());
    if (steps != null) {
      boolean endStepReached = false;
      int stepNumber = 0;
      CLIStep nextStep = steps[stepNumber];
      Result lastResult = null;
      SectionResultData nextStepArgs = null;
      while (!endStepReached) {
        try {
          Result result = executeStep(logWrapper, shell, nextStep, parseResult, nextStepArgs);
          String nextStepString = null;
          nextStepString = getNextStep(result);
          nextStepArgs = extractArgumentsForNextStep(result);          
          if (!"END".equals(nextStepString)) {
            String step = nextStepString;
            boolean stepFound = false;
            for (CLIStep s : steps)
              if (step.equals(s.getName())) {
                nextStep = s;
                stepFound = true;
              }
            if (!stepFound) {
              return ResultBuilder.buildResult(ResultBuilder.createErrorResultData().addLine(
                  "Wrong step name returned by previous step : " + step));
            }
          } else {
            lastResult = result;
            endStepReached = true;
          }

        } catch (CLIStepExecption e) {
          endStepReached = true;
          lastResult = e.getResult();
        }
      }
      return lastResult;
    } else {
      Gfsh.println("Command returned null steps");
      return ResultBuilder.buildResult(ResultBuilder.createErrorResultData().addLine(
          "Multi-step command Return NULL STEP Array"));
    }
  }

  private static Result executeStep(final LogWrapper logWrapper,
                                    final Gfsh shell,
                                    final CLIStep nextStep,
                                    final ParseResult parseResult,
                                    final SectionResultData nextStepArgs)
  {
    try {
      if (nextStep instanceof CLIRemoteStep) {        
        if (shell.isConnectedAndReady()) {
          if (GfshParseResult.class.isInstance(parseResult)) {
            GfshParseResult gfshParseResult = (GfshParseResult) parseResult;
            // this makes sure that "quit" step will correctly update the environment with empty stepArgs
            if (nextStepArgs != null) {
              GfJsonObject argsJSon = nextStepArgs.getSectionGfJsonObject();
              shell.setEnvProperty(CLIMultiStepHelper.STEP_ARGS, argsJSon.toString());
            }
            CommandRequest commandRequest = new CommandRequest(gfshParseResult, shell.getEnv());
            commandRequest.setCustomInput(changeStepName(gfshParseResult.getUserInput(), nextStep.getName()));
            commandRequest.getCustomParameters().put(CliStrings.QUERY__STEPNAME, nextStep.getName());

            String json = (String) shell.getOperationInvoker().processCommand(commandRequest);

            return ResultBuilder.fromJson(json);
          } else {
            throw new IllegalArgumentException("Command Configuration/Definition error.");
          }
        } else {
          try{throw new Exception();} catch (Exception ex) {ex.printStackTrace();}
          throw new IllegalStateException("Can't execute a remote command without connection. Use 'connect' first to connect.");
        }
      } else {
        Map<String, String> args = CommandExecutionContext.getShellEnv();
        if (args == null) {
          args = new HashMap<String, String>();
          CommandExecutionContext.setShellEnv(args);
        }
        if (nextStepArgs != null) {
          GfJsonObject argsJSon = nextStepArgs.getSectionGfJsonObject();
          Gfsh.getCurrentInstance().setEnvProperty(CLIMultiStepHelper.STEP_ARGS, argsJSon.toString());
        }
        return nextStep.exec();
      }
    } catch (CLIStepExecption e) {
      logWrapper.severe("CLIStep " + nextStep.getName() + " failed aborting command");
      throw e;
    }
  }

  private static String changeStepName(String userInput, String stepName) {
    int i = userInput.indexOf("--step-name=");    
    if (i == -1) {
      return userInput + " --step-name=" + stepName;
    } else {
      // TODO this is a dangerous assumption... to assume the "--step-name" query command option is the last parameter
      // specified!
      return userInput.substring(0, i) + "--step-name=" + stepName;
    }
  }

  public static SectionResultData extractArgumentsForNextStep(Result result) {
    CommandResult cResult = (CommandResult) result;
    if (ResultData.TYPE_COMPOSITE.equals(cResult.getType())) {
      CompositeResultData rd = (CompositeResultData) cResult.getResultData();
      SectionResultData data = rd.retrieveSection(CLIMultiStepHelper.ARG_SECTION);
      return data;
    } else {
      if (ResultData.TYPE_ERROR.equals(cResult.getType())) {
        throw new CLIStepExecption(cResult);
      } else {
        throw new StepExecutionException("Step returned result of type other than " + ResultData.TYPE_COMPOSITE + " Type "
            + cResult.getType());
      }
    }
  }

  public static String getNextStep(Result cdata) {
    CommandResult cResult = (CommandResult) cdata;
    if (ResultData.TYPE_COMPOSITE.equals(cResult.getType())) {
      CompositeResultData rd = (CompositeResultData) cResult.getResultData();
      SectionResultData section = rd.retrieveSection(CLIMultiStepHelper.STEP_SECTION);
      String nextStep = (String) section.retrieveObject(CLIMultiStepHelper.NEXT_STEP_NAME);
      return nextStep;
    } else {
      if (ResultData.TYPE_ERROR.equals(cResult.getType())) {
        throw new CLIStepExecption(cResult);
      } else {
        throw new RuntimeException("Step returned result of type other than " + ResultData.TYPE_COMPOSITE + " Type "
            + cResult.getType());
      }
    }
  }

  public static CommandResult getDisplayResultFromArgs(GfJsonObject args) {
    SectionResultData sectionData = new SectionResultData(args);
    CompositeResultData data = ResultBuilder.createCompositeResultData();
    data.addSection(sectionData);
    return (CommandResult) ResultBuilder.buildResult(data);
  }

  public static GfJsonObject getStepArgs() {
    Map<String, String> args = null;
    if(CliUtil.isGfshVM){
      args = Gfsh.getCurrentInstance().getEnv();
    }else{
      args = CommandExecutionContext.getShellEnv();
    }
    if (args == null)
      return null;
    String stepArg = args.get(CLIMultiStepHelper.STEP_ARGS);
    if (stepArg == null)
      return null;
    GfJsonObject object;
    try {
      object = new GfJsonObject(stepArg);
    } catch (GfJsonException e) {
      throw new RuntimeException("Error converting arguments section into json object");
    }
    return object;
  }

  public static Result createSimpleStepResult(String nextStep) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, nextStep);
    return ResultBuilder.buildResult(result);
  }
  
  public static Object chooseStep(CLIStep[] steps, String stepName) {
    if ("ALL".equals(stepName)) {
      return steps;
    } else {
      for (CLIStep s : steps)
        if (stepName.equals(s.getName())) {
          return s.exec();
        }
      return null;
    }
  }

  public static Result createStepSeqResult(CLIStep[] steps) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, steps[0].getName());
    String[] array = new String[steps.length];
    for (int i = 0; i < steps.length; i++) {
      array[i] = steps[i++].getName();
    }
    section.addData(NEXT_STEP_NAMES, array);
    return ResultBuilder.buildResult(result);
  }
  
  public static Result createEmptyResult(String step) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, step);
    return ResultBuilder.buildResult(result);
  }   

  public static Result createPageResult(String fields[], Object values[], String step, String[] header, Object[][] table) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, step);
    SectionResultData page = result.addSection(ARG_SECTION);

    if (fields.length != values.length)
      throw new RuntimeException("Fields array and its value arraylength dont match");
    for (int i = 0; i < fields.length; i++) {
      page.addData(fields[i], values[i]);
    }
    createPageTableAndBanner(page, header, table);
    return ResultBuilder.buildResult(result);
  }

  public static Result createPageResult(List<String> fields, @SuppressWarnings("rawtypes") List values, String step, String[] header, Object[][] table) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, step);
    SectionResultData page = result.addSection(ARG_SECTION);

    if (fields.size() != values.size())
      throw new RuntimeException("Fields array and its value arraylength dont match");
    for (int i = 0; i < fields.size(); i++) {
      page.addData(fields.get(i), values.get(i));
    }
    createPageTableAndBanner(page, header, table);
    return ResultBuilder.buildResult(result);
  }

  private static void createPageTableAndBanner(SectionResultData page, String[] header, Object[][] table) {
    TabularResultData resultData = page.addTable();
    int columns = header.length;
    for (int i = 0; i < table.length; i++) {
      int rowLength = table[i].length;
      if (rowLength != columns)
        throw new RuntimeException("Row contains more than " + columns + " :  " + rowLength);
    }

    for (int i = 0; i < table.length; i++) {
      for (int j = 0; j < columns; j++) {
        resultData.accumulate(header[j], table[i][j]);
      }
    }
  }

  public static Result createBannerResult(String fields[], Object values[], String step) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, step);
    SectionResultData page = result.addSection(ARG_SECTION);
    if (fields.length != values.length) {
      throw new RuntimeException("Fields array and its value arraylength dont match");
    }
    for (int i = 0; i < fields.length; i++) {
      page.addData(fields[i], values[i]);
    }
    return ResultBuilder.buildResult(result);
  }
  
  public static Result createBannerResult(List<String> fields, @SuppressWarnings("rawtypes") List values, String step) {
    CompositeResultData result = ResultBuilder.createCompositeResultData();
    SectionResultData section = result.addSection(STEP_SECTION);
    section.addData(NEXT_STEP_NAME, step);
    SectionResultData page = result.addSection(ARG_SECTION);
    if (fields.size() != values.size()) {
      throw new RuntimeException("Fields array and its value arraylength dont match");
    }
    for (int i = 0; i < fields.size(); i++) {
      page.addData(fields.get(i), values.get(i));
    }
    return ResultBuilder.buildResult(result);
  }

  public static void logFine(String msg) {
    Gfsh.println(msg);
    // TODO Use gemfire Logging for code path running on manager
  }
  
  public static abstract class LocalStep implements CLIStep{
    private String name=null;
    protected Object[] commandArguments = null;
    
    public LocalStep(String name, Object[] arguments){
      this.name = name;
      this.commandArguments = arguments;
    }
    
    public String getName(){
      return name;
    }    
  }
  
  @SuppressWarnings("serial")
  public static abstract class RemoteStep implements CLIRemoteStep{
    private String name=null;
    protected Object[] commandArguments = null;
    
    public RemoteStep(String name, Object[] arguments){
      this.name = name;
      this.commandArguments = arguments;
    }
    
    public String getName(){
      return name;
    }    
  }
  
  public static class StepExecutionException extends RuntimeException{
    private static final long serialVersionUID = 1L;
    private String message;
    
    public StepExecutionException(String message){
      LogWriter logger = CacheFactory.getAnyInstance().getLogger();
      logger.severe(message);
      this.message = message;
    }
    
    @Override
    public String getMessage(){
      return StepExecutionException.class.getName();
    }
    
    public String getStepExecutionExceptionMessage(){
      return message;
    }
    
  }
  

}
