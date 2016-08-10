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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.AbstractCliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.ListFunctionFunction;
import com.gemstone.gemfire.management.internal.cli.functions.UnregisterFunction;
import com.gemstone.gemfire.management.internal.cli.functions.UserFunctionExecution;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/**
 * 
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class FunctionCommands implements CommandMarker {
  private final ListFunctionFunction listFunctionFunction = new ListFunctionFunction();
  
  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }
  
  @CliCommand(value = CliStrings.EXECUTE_FUNCTION, help = CliStrings.EXECUTE_FUNCTION__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_FUNCTION })
  @ResourceOperation(resource = Resource.DATA, operation = Operation.WRITE)
  public Result executeFunction(
      //TODO: Add optioncontext for functionID
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ID, 
          mandatory = true, help = CliStrings.EXECUTE_FUNCTION__ID__HELP) String functionId,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ONGROUPS, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXECUTE_FUNCTION__ONGROUPS__HELP) String[] onGroups,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ONMEMBER, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.EXECUTE_FUNCTION__ONMEMBER__HELP) String onMember,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ONREGION, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          optionContext = ConverterHint.REGIONPATH,
          help = CliStrings.EXECUTE_FUNCTION__ONREGION__HELP) String onRegion,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ARGUMENTS, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          help = CliStrings.EXECUTE_FUNCTION__ARGUMENTS__HELP) String[] arguments,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          help = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR__HELP) String resultCollector,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__FILTER, 
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
          help = CliStrings.EXECUTE_FUNCTION__FILTER__HELP) String filterString) {

    Result result = null;
    CompositeResultData executeFunctionResultTable = ResultBuilder
        .createCompositeResultData();
    TabularResultData resultTable = executeFunctionResultTable.addSection().addTable(
        "Table1");
    String headerText = "Execution summary";
    resultTable.setHeader(headerText);
    ResultCollector resultCollectorInstance = null;
    Function function;
    Set<String> filters = new HashSet<String>();
    Execution execution = null;
    if (functionId != null){
      functionId = functionId.trim();
    }
    if (onRegion != null){
      onRegion = onRegion.trim();
    }
    if (onMember != null){
      onMember = onMember.trim();
    }
    if (filterString != null){
      filterString = filterString.trim();
    }

    try {
      // validate otherwise return right away. no need to process anything
      if (functionId == null || functionId.length() == 0) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__MISSING_FUNCTIONID);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      }

      if (onRegion != null && onMember != null && onGroups != null) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      } else if (onRegion != null && onMember != null) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      } else if (onMember != null && onGroups != null) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      } else if (onRegion != null && onGroups != null) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      } else if (onRegion != null && onMember != null && onGroups != null) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
                CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      }else if ( (onRegion == null || onRegion.length() == 0) && (filterString != null ) ){
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
        .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
            CliStrings.EXECUTE_FUNCTION__MSG__MEMBER_SHOULD_NOT_HAVE_FILTER_FOR_EXECUTION);
        result = ResultBuilder.buildResult(errorResultData);
        return result;
      }
      
      Cache cache = CacheFactory.getAnyInstance();   
     

      if (resultCollector != null) {
        resultCollectorInstance = (ResultCollector) ClassPathLoader.getLatest().forName(resultCollector).newInstance();        
      }

      if (filterString != null && filterString.length() > 0) {
        filters.add(filterString);
      }

      if (onRegion == null && onMember == null && onGroups == null) {
        // run function on all the members excluding locators bug#46113
        //if user wish to execute on locator then he can choose --member or --group option
        Set<DistributedMember> dsMembers = CliUtil.getAllNormalMembers(cache);
        if (dsMembers.size() > 0) {          
          function = new UserFunctionExecution();
          LogWrapper.getInstance().info(
              CliStrings.format(
                  CliStrings.EXECUTE_FUNCTION__MSG__EXECUTING_0_ON_ENTIRE_DS,
                  functionId));
          for (DistributedMember member : dsMembers) {
            executeAndGetResults(functionId, filterString, resultCollector,
                arguments, cache, member, resultTable, onRegion);
          }
          return ResultBuilder.buildResult(resultTable);
        } else {
          return ResultBuilder
              .createUserErrorResult(CliStrings.EXECUTE_FUNCTION__MSG__DS_HAS_NO_MEMBERS);
        }
      } else if (onRegion != null && onRegion.length() > 0) {
        if (cache.getRegion(onRegion) == null) {
          // find a member where region is present
          DistributedRegionMXBean bean = ManagementService
              .getManagementService(GemFireCacheImpl.getInstance())
              .getDistributedRegionMXBean(onRegion);
          if (bean == null) {
            bean = ManagementService.getManagementService(
                GemFireCacheImpl.getInstance()).getDistributedRegionMXBean(Region.SEPARATOR + onRegion);

            if (bean == null) {
              return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                  CliStrings.EXECUTE_FUNCTION__MSG__MXBEAN_0_FOR_NOT_FOUND,
                  onRegion));
            }
          }

          DistributedMember member = null;
          String[] membersName = bean.getMembers();
          Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);
          Iterator it = dsMembers.iterator();
          boolean matchFound = false;

          if (membersName.length > 0) {
            while (it.hasNext() && matchFound == false) {
              DistributedMember dsmember = (DistributedMember) it.next();
              for (String memberName : membersName) {
                if (MBeanJMXAdapter.getMemberNameOrId(dsmember).equals(
                    memberName)) {
                  member = dsmember;
                  matchFound = true;
                  break;
                }
              }
            }
          }
          if (matchFound == true) {
            executeAndGetResults(functionId, filterString, resultCollector,
                arguments, cache, member, resultTable, onRegion);
            return ResultBuilder.buildResult(resultTable);
          } else {
            return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                CliStrings.EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER_REGION,
                " " + onRegion));
          }
        } else {
          execution = FunctionService.onRegion(cache.getRegion(onRegion));
          if (execution != null) {
            if (resultCollectorInstance != null) {
              execution = execution.withCollector(resultCollectorInstance);
            }
            if (filters != null && filters.size() > 0) {
              execution = execution.withFilter(filters);
            }
            if (arguments != null && arguments.length > 0) {
              execution = execution.withArgs(arguments);
            }

            try {
              List<Object> results = (List<Object>) execution.execute(
                  functionId).getResult();
              if (results.size() > 0) {
                StringBuilder strResult = new StringBuilder();
                for (Object obj : results) {
                  strResult.append(obj);
                }
                toTabularResultData(resultTable, cache.getDistributedSystem()
                    .getDistributedMember().getId(), strResult.toString());
              }
              return ResultBuilder.buildResult(resultTable);
            } catch (FunctionException e) {
              return ResultBuilder
                  .createGemFireErrorResult(CliStrings
                      .format(
                          CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_REGION_1_DETAILS_2,
                          functionId, onRegion, e.getMessage()));
            }
          } else {
            return ResultBuilder
                .createGemFireErrorResult(CliStrings
                    .format(
                        CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_REGION_1_DETAILS_2,
                        functionId,
                        onRegion,
                        CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR));
          }
        }
      } else if (onGroups != null) {
        // execute on group members
        Set<DistributedMember> dsMembers = new HashSet<DistributedMember>();
        for(String grp : onGroups){
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
        
        StringBuilder successMessage = new StringBuilder();
        if (dsMembers.size() > 0) {
          for (DistributedMember member : dsMembers) {
            executeAndGetResults(functionId, filterString, resultCollector,
                arguments, cache, member, resultTable, onRegion);
          }
          return ResultBuilder.buildResult(resultTable);
        } else {
          StringBuilder grps = new StringBuilder();
          for(String grp : onGroups){
            grps.append(grp);
            grps.append(", ");
          }
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.EXECUTE_FUNCTION__MSG__GROUPS_0_HAS_NO_MEMBERS, grps.toString().substring(0,grps.toString().length() -1)));
        }
      } else if (onMember != null && onMember.length() > 0) {
        DistributedMember member = CliUtil
            .getDistributedMemberByNameOrId(onMember); // fix for bug 45658
        if (member != null) {
          executeAndGetResults(functionId, filterString, resultCollector,
              arguments, cache, member, resultTable, onRegion);
        } else {
          toTabularResultData(resultTable, onMember, CliStrings
              .format(CliStrings.EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER
                  + " " + onMember));
        }
        return ResultBuilder.buildResult(resultTable);
      }
    } catch (InstantiationException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
          .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    } catch (IllegalAccessException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
          .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    } catch (IllegalArgumentException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
          .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    } catch (SecurityException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
          .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    } catch (Exception e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
      .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
      .addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    }

    return result;
  }

  DistributedMember getMember(Cache cache, String memberNameOrId) {
    DistributedMember member = null;
    Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache) ;
    Iterator<DistributedMember> it = dsMembers.iterator();
    while (it.hasNext()) {
      DistributedMember tempMember = (DistributedMember) it.next();
      if (memberNameOrId.equals(tempMember.getId())
          || memberNameOrId.equals(tempMember.getName())) {
        return tempMember;
      }
    }
    return member;
  }

  void executeAndGetResults(String functionId, String filterString,
      String resultCollector, String[] arguments, Cache cache,
      DistributedMember member, TabularResultData resultTable, String onRegion) {
    StringBuilder resultMessege = new StringBuilder();
    try {      
      Function function = new UserFunctionExecution();
      Object[] args = new Object[5];
      args[0] = functionId;
      if(filterString != null){
        args[1] = filterString;
      }
      if (resultCollector != null) {
        args[2] = resultCollector;
      }
      if (arguments != null && arguments.length > 0) {        
        args[3] = new String();
        for (String str : arguments) {
          // send via CSV separated value format          
          if(str != null){
            args[3] = args[3] + str + ",";            
          }
        }
      }
      args[4] = onRegion;

      Execution execution = FunctionService.onMember(member).withArgs(args);
      if (execution != null) {
        List<Object> results = (List<Object>) execution.execute(function)
            .getResult();
        if (results != null) {
          for (Object resultObj : results) {
            if (resultObj != null) {
              if (resultObj instanceof String) {
                resultMessege.append(((String) resultObj));
              } else if (resultObj instanceof Exception) {
                resultMessege.append(((Exception)resultObj).getMessage());
              } else {
                resultMessege.append(resultObj);
              }
            }
          }
        }
        toTabularResultData(resultTable, member.getId(), resultMessege
            .toString());
      }else{
        toTabularResultData(resultTable, member.getId(), CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR);        
      }
    } catch (FunctionException e) {
      resultMessege.append(CliStrings.format(
          CliStrings.EXECUTE_FUNCTION__MSG__COULD_NOT_EXECUTE_FUNCTION_0_ON_MEMBER_1_ERROR_2, functionId,
          member.getId(), e.getMessage()));
      toTabularResultData(resultTable, member.getId(), resultMessege
          .toString());
    } catch (Exception e) {
      resultMessege.append(CliStrings.format(
          CliStrings.EXECUTE_FUNCTION__MSG__COULD_NOT_EXECUTE_FUNCTION_0_ON_MEMBER_1_ERROR_2, functionId,
          member.getId(), e.getMessage()));
      toTabularResultData(resultTable, member.getId(), resultMessege
          .toString());
    }
  }

  protected void toTabularResultData(TabularResultData table,
      String memberId, String memberResult) {
    String newLine = System.getProperty("line.separator");
    table.accumulate("Member ID/Name", memberId);
    table.accumulate("Function Execution Result", memberResult);
  }
  
  @CliCommand(value = CliStrings.DESTROY_FUNCTION, help = CliStrings.DESTROY_FUNCTION__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_FUNCTION } ,
      interceptor = "com.gemstone.gemfire.management.internal.cli.commands.FunctionCommands$Interceptor")  
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  //TODO: Add optioncontext for functionId
  public Result destroyFunction(
      @CliOption(key = CliStrings.DESTROY_FUNCTION__ID, 
                 mandatory = true, help = CliStrings.DESTROY_FUNCTION__HELP) String functionId,
      @CliOption(key = CliStrings.DESTROY_FUNCTION__ONGROUPS, 
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                 optionContext = ConverterHint.MEMBERGROUP,
                 help = CliStrings.DESTROY_FUNCTION__ONGROUPS__HELP) String[] groups,
      @CliOption(key = CliStrings.DESTROY_FUNCTION__ONMEMBER, 
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                 optionContext = ConverterHint.MEMBERIDNAME,
                 help = CliStrings.DESTROY_FUNCTION__ONMEMBER__HELP) String memberId) {
    Result result = null;
    try {    
      Cache cache = CacheFactory.getAnyInstance();
      Set<DistributedMember> dsMembers = new HashSet<DistributedMember>();
      if(groups != null && memberId != null){
        return ResultBuilder.createUserErrorResult(CliStrings.DESTROY_FUNCTION__MSG__PROVIDE_OPTION);
      }else if (groups != null && groups.length > 0 ) {   
        //execute on group members
        for(String grp : groups){
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
        @SuppressWarnings("unchecked")
        Result results = executeFunction(cache,  dsMembers,  functionId);
        return results;
      }else if (memberId != null){
        //execute on member
        dsMembers.add(getMember(cache , memberId));
         @SuppressWarnings("unchecked")
        Result results =  executeFunction(cache,  dsMembers,  functionId);   
        return results;           
      }  
      else{
        //no option provided. 
        @SuppressWarnings("unchecked")
        Result results =  executeFunction(cache,  cache.getMembers(),  functionId);        
        return results;   
      }
    } catch (Exception e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
              e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
      return result;
    }

  }
  /**
   * Interceptor used by gfsh to intercept execution of destroy.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();
      Set< Entry<String, String> > setEnvMap = paramValueMap.entrySet();    
      String onGroup = paramValueMap.get(CliStrings.DESTROY_FUNCTION__ONGROUPS);
      String onMember = paramValueMap.get(CliStrings.DESTROY_FUNCTION__ONMEMBER);
      
      if ((onGroup == null && onMember == null ) ){      
        Response response = readYesNo("Do you really want to destroy "+paramValueMap.get(CliStrings.DESTROY_FUNCTION__ID)+ " on entire DS?", Response.NO);
        if (response == Response.NO) {          
          return ResultBuilder.createShellClientAbortOperationResult("Aborted destroy of " +paramValueMap.get(CliStrings.DESTROY_FUNCTION__ID) );
        }else{          
          return ResultBuilder.createInfoResult("Destroying " +paramValueMap.get(CliStrings.DESTROY_FUNCTION__ID) );
        }
      }else{        
        return ResultBuilder.createInfoResult("Destroying " +paramValueMap.get(CliStrings.DESTROY_FUNCTION__ID) );
      }     
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {     
      return commandResult;
    }
  }
  
  Result executeFunction(Cache cache, Set<DistributedMember> DsMembers, String functionId){
    //unregister on a set of of members
    Function unregisterFunction = new UnregisterFunction();
    FunctionService.registerFunction(unregisterFunction);
    List resultList = null;
    
    if(DsMembers.isEmpty()){      
     return  ResultBuilder.createInfoResult("No members for execution");
    }
    Object[] obj = new Object[1];
    obj[0] = functionId;
    
    Execution execution = FunctionService.onMembers(DsMembers).withArgs(obj);

    if (execution == null) {
      cache.getLogger().error("executeUnregister execution is null") ;
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
              CliStrings.DESTROY_FUNCTION__MSG__CANNOT_EXECUTE);
      return (ResultBuilder.buildResult(errorResultData));
    }
    try {
      resultList = (ArrayList) execution.execute(unregisterFunction)
          .getResult();
    } catch (FunctionException ex) {     
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(
              ex.getMessage());
      return (ResultBuilder.buildResult(errorResultData));
    }
    String resultStr = ((String) resultList.get(0));    
    if (resultStr.equals("Succeeded in unregistering")){
      StringBuilder members = new StringBuilder();
      for(DistributedMember member : DsMembers){
        members.append(member.getId());
        members.append(",");
      }
      return ResultBuilder.createInfoResult("Destroyed " + functionId
        + " Successfully on " + members.toString().substring(0,members.toString().length() -1));       
    }else{
      return ResultBuilder.createInfoResult("Failed in unregistering");
    }
  }     
  
  @CliCommand(value = CliStrings.LIST_FUNCTION, help = CliStrings.LIST_FUNCTION__HELP)
  @CliMetaData(relatedTopic = { CliStrings.TOPIC_GEODE_FUNCTION })
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listFunction(
      @CliOption(key = CliStrings.LIST_FUNCTION__MATCHES, 
                 help = CliStrings.LIST_FUNCTION__MATCHES__HELP)String matches,
      @CliOption(key = CliStrings.LIST_FUNCTION__GROUP, 
                 optionContext = ConverterHint.MEMBERGROUP,
                 help = CliStrings.LIST_FUNCTION__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String groups,
      @CliOption(key = CliStrings.LIST_FUNCTION__MEMBER, 
                 optionContext = ConverterHint.MEMBERIDNAME,
                 help = CliStrings.LIST_FUNCTION__MEMBER__HELP)
      @CliMetaData (valueSeparator = ",") String members) {
    TabularResultData tabularData = ResultBuilder.createTabularResultData();
    boolean accumulatedData = false;

    Cache cache = CacheFactory.getAnyInstance();

    Set<DistributedMember> targetMembers;
    try {
      targetMembers = CliUtil.findAllMatchingMembers(groups, members);
    } catch (CommandResultException crex) {
      return crex.getResult();
    }
    
    try {
      ResultCollector<?, ?> rc = CliUtil.executeFunction(this.listFunctionFunction, new Object[] { matches }, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
      
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Function", "<ERROR: " + result.getThrowable().getMessage() + ">");
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else if (result.isSuccessful()) {
          String[] strings = (String[]) result.getSerializables();
          Arrays.sort(strings);
          for (int i = 0; i < strings.length; i++) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Function", strings[i]);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.LIST_FUNCTION__NO_FUNCTIONS_FOUND_ERROR_MESSAGE);
      }
      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to list functions: " + th.getMessage());
    }
  }
  
  @CliAvailabilityIndicator({CliStrings.EXECUTE_FUNCTION, CliStrings.DESTROY_FUNCTION, CliStrings.LIST_FUNCTION})  
  public boolean functionCommandsAvailable() {
    boolean isAvailable = true; //always available on server
    if (CliUtil.isGfshVM()) { //in gfsh check if connected
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;  
  }
}
