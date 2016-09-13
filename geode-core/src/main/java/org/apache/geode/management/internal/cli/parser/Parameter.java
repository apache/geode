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
package org.apache.geode.management.internal.cli.parser;

import org.springframework.shell.core.Converter;

import org.apache.geode.management.internal.cli.parser.preprocessor.PreprocessorUtils;

/**
 * Parameter of a Command
 * 
 * @since GemFire 7.0
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
