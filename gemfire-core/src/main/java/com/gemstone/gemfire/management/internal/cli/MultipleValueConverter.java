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
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * Extends {@link Converter} to add multiple value support
 * 
 * @author Nikhil Jadhav
 * 
 * @param <T>
 */
public interface MultipleValueConverter<T> extends Converter<T> {

  /**
   * Similar to {@link Converter#convertFromText(String, Class, String)} but
   * with support for multiple values
   * 
   * @param value
   * @param targetType
   * @param context
   * @return required Data 
   */
  T convertFromText(String[] value, Class<?> targetType, String context);

  /**
   * Similar to
   * {@link Converter#getAllPossibleValues(List, Class, String, String, MethodTarget)}
   * but with support for multiple values
   * 
   * @param completions
   * @param targetType
   * @param existingData
   * @param context
   * @param target
   * @return required Data
   */
  boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target);
}
