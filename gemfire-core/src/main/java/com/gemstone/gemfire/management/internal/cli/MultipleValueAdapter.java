/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;

public abstract class MultipleValueAdapter<T> implements MultipleValueConverter<T>{

  @Override
  public T convertFromText(String value, Class<?> targetType,
      String optionContext) {
    return null;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    return false;
  }

}
