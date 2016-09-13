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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.domain.CacheServerInfo;
import com.gemstone.gemfire.management.internal.cli.domain.MemberInformation;
import com.gemstone.gemfire.management.internal.cli.functions.GetMemberInformationFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
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

/***
 *
 *
 * @since GemFire 7.0
 */
public class MemberCommands implements CommandMarker {
  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }
  private static final GetMemberInformationFunction getMemberInformation = new GetMemberInformationFunction();

  @CliCommand(value = { CliStrings.LIST_MEMBER }, help = CliStrings.LIST_MEMBER__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listMember(
		  @CliOption(key = { CliStrings.LIST_MEMBER__GROUP },
		             unspecifiedDefaultValue = "",
		             optionContext = ConverterHint.MEMBERGROUP,
		             help = CliStrings.LIST_MEMBER__GROUP__HELP)
                  String group) {
    Result result = null;

    //TODO: Add the code for identifying the system services
    try {
      Set<DistributedMember> memberSet = new TreeSet<DistributedMember>();
      Cache cache = CacheFactory.getAnyInstance();

      //default get all the members in the DS
      if (group.isEmpty()) {
        memberSet.addAll(CliUtil.getAllMembers(cache));
      } else {
        memberSet.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }

      if (memberSet.isEmpty()) {
        result = ResultBuilder.createInfoResult(CliStrings.LIST_MEMBER__MSG__NO_MEMBER_FOUND);
      } else {
        TabularResultData resultData = ResultBuilder.createTabularResultData();
        Iterator<DistributedMember> memberIters = memberSet.iterator();
        while (memberIters.hasNext()) {
          DistributedMember member = memberIters.next();
          resultData.accumulate("Name", member.getName());
          resultData.accumulate("Id", member.getId());
        }

        result = ResultBuilder.buildResult(resultData);
      }
    } catch (Exception e) {

      result = ResultBuilder.createGemFireErrorResult("Could not fetch the list of members. "+e.getMessage());
      LogWrapper.getInstance().warning(e.getMessage(), e);
    }

    return result;
  }

  @CliCommand(value = { CliStrings.DESCRIBE_MEMBER }, help = CliStrings.DESCRIBE_MEMBER__HELP)
  @CliMetaData(shellOnly = false, relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result describeMember(
  	@CliOption(key = CliStrings.DESCRIBE_MEMBER__IDENTIFIER,
  	             optionContext = ConverterHint.ALL_MEMBER_IDNAME,
		  					 help = CliStrings.DESCRIBE_MEMBER__HELP,
		  					 mandatory=true)
                  String memberNameOrId) {
  	Result result = null;

  	try {
      DistributedMember memberToBeDescribed = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);

      if (memberToBeDescribed != null) {
        //Abhishek - This information should be available through the MBeans too. We might not need the function.
        //Sourabh - Yes, but then the command is subject to Mbean availability, which would be affected once MBean filters are used.

        ResultCollector <?, ?> rc = CliUtil.executeFunction(getMemberInformation, null, memberToBeDescribed);

        ArrayList<?> output = (ArrayList<?>) rc.getResult();
        Object obj = output.get(0);

        if (obj != null && (obj instanceof MemberInformation) ) {

          CompositeResultData crd = ResultBuilder.createCompositeResultData();

        	MemberInformation memberInformation = (MemberInformation) obj;
        	memberInformation.setName(memberToBeDescribed.getName());
        	memberInformation.setId(memberToBeDescribed.getId());
        	memberInformation.setHost(memberToBeDescribed.getHost());
        	memberInformation.setProcessId(""+memberToBeDescribed.getProcessId());

        	SectionResultData section = crd.addSection();
        	section.addData("Name", memberInformation.getName());
        	section.addData("Id", memberInformation.getId());
        	section.addData("Host", memberInformation.getHost());
        	section.addData("Regions", CliUtil.convertStringSetToString(memberInformation.getHostedRegions(), '\n'));
        	section.addData("PID", memberInformation.getProcessId());
        	section.addData("Groups", memberInformation.getGroups());
        	section.addData("Used Heap", memberInformation.getHeapUsage() + "M");
        	section.addData("Max Heap", memberInformation.getMaxHeapSize() + "M");
        	{
        	  String offHeapMemorySize = memberInformation.getOffHeapMemorySize();
        	  if (offHeapMemorySize != null && !offHeapMemorySize.isEmpty()) {
        	    section.addData("Off Heap Size", offHeapMemorySize);
        	  }
        	}
        	section.addData("Working Dir", memberInformation.getWorkingDirPath());
        	section.addData("Log file", memberInformation.getLogFilePath());

        	section.addData("Locators", memberInformation.getLocators());

        	if (memberInformation.isServer()) {
        	  SectionResultData clientServiceSection = crd.addSection();
        	  List<CacheServerInfo> csList = memberInformation.getCacheServeInfo();

        	  if (csList != null) {
        	    Iterator<CacheServerInfo> iters = csList.iterator();
              clientServiceSection.setHeader("Cache Server Information");

              while (iters.hasNext()) {
                CacheServerInfo cacheServerInfo = iters.next();
                clientServiceSection.addData("Server Bind",cacheServerInfo.getBindAddress());
                clientServiceSection.addData("Server Port" , cacheServerInfo.getPort());
                clientServiceSection.addData("Running" , cacheServerInfo.isRunning());
              }

              clientServiceSection.addData("Client Connections", memberInformation.getClientCount());
        	  }
        	}
        	result = ResultBuilder.buildResult(crd);

        } else {
          result = ResultBuilder.createInfoResult(CliStrings.format(CliStrings.DESCRIBE_MEMBER__MSG__INFO_FOR__0__COULD_NOT_BE_RETRIEVED, new Object[] {memberNameOrId}));
        }
      } else {
        result = ResultBuilder.createInfoResult(CliStrings.format(CliStrings.DESCRIBE_MEMBER__MSG__NOT_FOUND, new Object[] {memberNameOrId}));
      }
    } catch (CacheClosedException e) {

    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }

  @CliAvailabilityIndicator({CliStrings.LIST_MEMBER, CliStrings.DESCRIBE_MEMBER})
  public boolean isListMemberAvailable() {
    boolean isAvailable = true;
    if (CliUtil.isGfshVM()) {
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }

}
