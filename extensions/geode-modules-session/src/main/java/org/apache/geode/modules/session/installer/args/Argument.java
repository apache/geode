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

package com.gemstone.gemfire.modules.session.installer.args;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a single command line argument.
 */
public class Argument {

  /**
   * Parameter names.
   */
  private final String[] paramNames;

  /**
   * Default values for the parameters when not explicitly set.
   */
  private String[] defaults;

  /**
   * Environment variable names forfor each parameter where values will be
   * pulled in, if not explicitly provided and if the environment variable
   * exists.
   */
  private String[] envVars;

  /**
   * Flag indicating whether this argument is required on the command line.
   */
  private final boolean required;

  /**
   * Handler used to hook into processing.
   */
  private ArgumentHandler handler;

  /**
   * List of all representation forms.
   */
  private final List<String> forms = new ArrayList<String>();

  /**
   * Usage description.
   */
  private String description;

  ///////////////////////////////////////////////////////////////////////////
  // Constructor:

  /**
   * Contructor to create an argument definition.
   *
   * @param primaryForm    the form of the argument (e.g., --foo).  Should start
   *                       with a dash.
   * @param argRequired    flag indicating whether or not the argument is
   *                       required to be onthe command line
   * @param parameterNames names of the parameters to this argument for use in
   *                       the usage generation
   */
  public Argument(
      final String primaryForm,
      final boolean argRequired,
      final String... parameterNames) {
    forms.add(primaryForm);
    paramNames = parameterNames;
    required = argRequired;
  }

  /**
   * Returns the number of parameters that this argument takes.
   *
   * @return parameter count
   */
  public int getParameterCount() {
    return paramNames.length;
  }

  /**
   * Returns the name of the parameter position requested.
   *
   * @param idx parameter index
   * @return parameter name
   */
  public String getParameterName(final int idx) {
    return paramNames[idx];
  }

  /**
   * Returns whether or not this argument is required to be defined.
   *
   * @return true if required, false if optional
   */
  public boolean isRequired() {
    return required;
  }

  /**
   * Determines if the argument provisioning has been done via the environment.
   */
  public boolean isDefinedInEnv() {
    if (envVars == null || paramNames.length == 0) {
      return false;
    }
    for (String var : envVars) {
      if (System.getenv(var) == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sets the argument handler.
   *
   * @param aHandler argument handler
   * @return this argument (for chained calls)
   */
  public Argument setArgumentHandler(final ArgumentHandler aHandler) {
    handler = aHandler;
    return this;
  }

  /**
   * Returns the argument handler.
   *
   * @return argument handler
   */
  public ArgumentHandler getArgumentHandler() {
    return handler;
  }

  /**
   * Adds a possible representation of the command line argument.
   *
   * @param aliasName additional form to accept
   * @return this argument (for chained calls)
   */
  public Argument addForm(final String aliasName) {
    forms.add(aliasName);
    return this;
  }

  /**
   * Returns the primary form of the argument.
   *
   * @return primary form
   */
  public String getPrimaryForm() {
    if (forms.isEmpty()) {
      return null;
    } else {
      return forms.get(0);
    }
  }

  /**
   * Returns a list of all valid representations of this command line argument.
   *
   * @return list of all registered forms
   */
  public List<String> getForms() {
    return forms;
  }

  /**
   * Sets a usage description for this argument.
   *
   * @param str usage description
   * @return this argument (for chained calls)
   */
  public Argument setDescription(final String str) {
    description = str;
    return this;
  }

  /**
   * Returns a usage description of this argument.
   *
   * @return description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets the default values when no explicit values were provided.
   *
   * @param newDefaults default values for all argument parameters
   * @return this argument (for chained calls)
   */
  public Argument setDefaults(final String... newDefaults) {
    if (newDefaults.length != paramNames.length) {
      throw (new IllegalArgumentException(
          "Defaults array length provided is not the correct size"));
    }
    defaults = newDefaults;
    return this;
  }

  /**
   * Returns the defaults.
   *
   * @return default parameter values
   */
  public String[] getDefaults() {
    return defaults;
  }

  /**
   * Sets the environment variables which will be checked for values before
   * falling back on the default values.
   *
   * @param newEnvVars environment variable name array
   * @return this argument (for chained calls)
   */
  public Argument setEnvVars(final String... newEnvVars) {
    if (newEnvVars.length != paramNames.length) {
      throw (new IllegalArgumentException(
          "Environment variables array length provided is not "
              + "the correct size"));
    }
    envVars = newEnvVars;
    return this;
  }

  /**
   * Returns the environment variable names for each parameter.
   *
   * @return environment variable names
   */
  public String[] getEnvVars() {
    return envVars;
  }

  /**
   * Returns a human readable form.
   *
   * @return human readable string
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("[Argument '");
    builder.append(forms.get(0));
    builder.append("'");
    if (paramNames.length > 0) {
      for (int i = 0; i < paramNames.length; i++) {
        builder.append(" <");
        builder.append(paramNames[i]);
        builder.append(">");
      }
    }
    builder.append("]");
    return builder.toString();
  }

}
