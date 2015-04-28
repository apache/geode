/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.management.cli.ConverterHint;

/**
 * 
 * @author Sourabh Bansod 
 * @since 7.0
 */
public class LogLevelConverter implements Converter<String> {
  private Set<Completion> logLevels;

  public LogLevelConverter() {
    logLevels = new LinkedHashSet<Completion>();
    int[] alllevels = LogWriterImpl.allLevels;
    for (int level : alllevels) {
      logLevels.add(new Completion(LogWriterImpl.levelToString(level)));
    }
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
   return String.class.equals(type) && ConverterHint.LOG_LEVEL.equals(optionContext);
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
    completions.addAll(logLevels);
    return !completions.isEmpty();
  }
}
