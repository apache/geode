/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.MultipleValueAdapter;

/**
 * @author Nikhil Jadhav
 *
 * @since 7.0
 * 
 * 
 */
public class StringArrayConverter extends MultipleValueAdapter<String[]>{

  @Override
  public String[] convertFromText(String[] value, Class<?> targetType,
      String context) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target) {
    return false;
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    if (String[].class.isAssignableFrom(type)
        && !optionContext.equals(ConverterHint.DIRS)) {
      return true;
    } else {
      return false;
    }
  }

}
