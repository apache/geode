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
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * 
 * @since GemFire 7.0
 */
public class MemberGroupConverter implements Converter<String> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
//    System.out.println("MemberGroupConverter.supports("+type+","+optionContext+")");
    return String.class.equals(type) && ConverterHint.MEMBERGROUP.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType,
      String optionContext) {
//    System.out.println("MemberGroupConverter.convertFromText("+value+","+targetType+","+optionContext+")");
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
//    System.out.println("MemberGroupConverter.getAllPossibleValues("+existingData+","+targetType+","+optionContext+")");
    if (String.class.equals(targetType) && ConverterHint.MEMBERGROUP.equals(optionContext)) {
      String[] memberGroupNames = getMemberGroupNames();
      
      for (String memberGroupName : memberGroupNames) {
        completions.add(new Completion(memberGroupName));
      }
    }
    return !completions.isEmpty();
  }

  public Set<String> getMemberGroups() {
    final Gfsh gfsh = Gfsh.getCurrentInstance();
    final Set<String> memberGroups = new TreeSet<String>();

    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final String[] memberGroupsArray = gfsh.getOperationInvoker().getDistributedSystemMXBean().listGroups();
      memberGroups.addAll(Arrays.asList(memberGroupsArray));
    }

    return memberGroups;
  }

  private String[] getMemberGroupNames() {
    final Set<String> memberGroups = getMemberGroups();
    return memberGroups.toArray(new String[memberGroups.size()]);
  }

}
