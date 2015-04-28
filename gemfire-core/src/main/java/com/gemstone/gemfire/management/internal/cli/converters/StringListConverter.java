/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.MultipleValueAdapter;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class StringListConverter extends MultipleValueAdapter<List<String>> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return List.class.isAssignableFrom(type) && ConverterHint.STRING_LIST.equals(optionContext);
  }

  @Override
  public List<String> convertFromText(String[] value, Class<?> targetType,
      String context) {
    List<String> list = null;

    if (List.class.isAssignableFrom(targetType) && ConverterHint.STRING_LIST.equals(context) && value != null && value.length > 0) {
      list = new ArrayList<String>(Arrays.asList(value));
    }
    return list;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target) {
    return List.class.isAssignableFrom(targetType) && ConverterHint.STRING_LIST.equals(context);
  }

}
