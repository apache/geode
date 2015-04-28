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
import java.util.Set;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.CommandManager;

/**
 * 
 * @author Abhishek Chaudhari 
 * @since 7.0
 */
public class HintTopicConverter implements Converter<String> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.HINTTOPIC.equals(optionContext);
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
    if (String.class.equals(targetType) && ConverterHint.HINTTOPIC.equals(optionContext)) {
      CommandManager commandManager = CommandManager.getExisting();
      if (commandManager != null) {
        Set<String> topicNames = commandManager.getTopicNames();
        
        for (String topicName : topicNames) {
          if (existingData != null && !existingData.isEmpty()) {
            if (topicName.startsWith(existingData)) { // match exact case
              completions.add(new Completion(topicName));
            } else if (topicName.toLowerCase().startsWith(existingData.toLowerCase())) {  // match case insensitive
              String completionStr = existingData + topicName.substring(existingData.length());
              
              completions.add(new Completion(completionStr));
            }
          } else {
            completions.add(new Completion(topicName));
          }
        }
      }
    }

    return !completions.isEmpty();
  }
}
