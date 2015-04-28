/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.management.cli.ConverterHint;

/***
 * Added converter to enable auto-completion for index-type
 * @author Sourabh Bansod
 *
 */
public class IndexTypeConverter implements Converter<String>{

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.INDEX_TYPE.equals(optionContext);
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
    
    if (String.class.equals(targetType) && ConverterHint.INDEX_TYPE.equals(optionContext)) {
      completions.add(new Completion("range"));
      completions.add(new Completion("key"));
      completions.add(new Completion("hash"));
    }
    return !completions.isEmpty();
  }
}
