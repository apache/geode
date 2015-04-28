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
 *
 * @since 8.0
 */
public class ClusterMemberIdNameConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.ALL_MEMBER_IDNAME.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType,
      String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.ALL_MEMBER_IDNAME.equals(optionContext)) {
      Set<String> memberIdAndNames = getMemberIdAndNames();

      for (String string : memberIdAndNames) {
        completions.add(new Completion(string));
      }
    }

    return !completions.isEmpty();
  }

  private Set<String> getMemberIdAndNames() {
    final Set<String> memberIdsAndNames = new TreeSet<String>();

    final Gfsh gfsh = Gfsh.getCurrentInstance();

    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final String[] memberIds = gfsh.getOperationInvoker().getDistributedSystemMXBean().listMembers();

      if (memberIds != null && memberIds.length != 0) {
        memberIdsAndNames.addAll(Arrays.asList(memberIds));
      }
    }

    return memberIdsAndNames;
  }

}
