/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import org.springframework.shell.core.Converter;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;

/**
 * Parameter of a Command
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 */
public abstract class Parameter {
  // help for the Parameter
  protected String help;

  // Constraint on the Parameter
  protected boolean required;

  // Useful for Value conversion
  protected String context;
  protected Converter<?> converter;

  // Data type of the option
  protected Class<?> dataType;

  // Necessary for preserving order in
  // ParseResult object
  protected int parameterNo;

  // value related
  protected boolean systemProvided;
  protected String unspecifiedDefaultValue;

  public String getHelp() {
    return help;
  }

  public void setHelp(String help) {
    this.help = help;
  }

  public boolean isRequired() {
    return required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public String getContext() {
    return context;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public Converter<?> getConverter() {
    return converter;
  }

  // TODO Change for concurrent access.
  public void setConverter(Converter<?> converter) {
    this.converter = converter;
  }

  public Class<?> getDataType() {
    return dataType;
  }

  public void setDataType(Class<?> dataType) {
    this.dataType = dataType;
  }

  public int getParameterNo() {
    return parameterNo;
  }

  public void setParameterNo(int parameterNo) {
    this.parameterNo = parameterNo;
  }

  public boolean isSystemProvided() {
    return systemProvided;
  }

  public void setSystemProvided(boolean systemProvided) {
    this.systemProvided = systemProvided;
  }

  public String getUnspecifiedDefaultValue() {
    if (unspecifiedDefaultValue.equals("__NULL__")) {
      return null;
    } else {
      return unspecifiedDefaultValue;
    }
  }

  public void setUnspecifiedDefaultValue(String unspecifiedDefaultValue) {
    this.unspecifiedDefaultValue = PreprocessorUtils.trim(unspecifiedDefaultValue).getString();
  }
}
