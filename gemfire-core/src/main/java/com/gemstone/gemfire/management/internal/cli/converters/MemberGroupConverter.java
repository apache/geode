/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
 * @author Abhishek Chaudhari 
 * @since 7.0
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
