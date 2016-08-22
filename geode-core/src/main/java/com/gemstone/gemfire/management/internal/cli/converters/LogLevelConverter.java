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
 * @since GemFire 7.0
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
