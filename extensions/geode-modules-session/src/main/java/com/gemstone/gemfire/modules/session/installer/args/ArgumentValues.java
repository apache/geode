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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Result object capturing the result of processing command line arguments.
 */
public class ArgumentValues {

  /**
   * Storage location for all arguments found after the "--" pseudo-arg.
   */
  private String[] postArgs = new String[]{};

  /**
   * Storage location for the command line argument values.
   */
  private final Map<Argument, List<String[]>> values =
      new LinkedHashMap<Argument, List<String[]>>();

  /**
   * Constructor.
   */
  ArgumentValues() {
    // Empty.
  }

  /**
   * Sets the post-arguments found after the "--" pseudo-argument.
   *
   * @param newPostArgs arguments defined after the special "--" argument
   */
  void setPostArgs(final String[] newPostArgs) {
    postArgs = newPostArgs;
  }

  /**
   * After processing the command line arguments, this method may be used to
   * return all arguments which were excluded from processing by their placement
   * after the "<code>--</code>" psuedo-argument.
   *
   * @return all unprocess arguments
   */
  public String[] getPostArgs() {
    return postArgs;
  }

  /**
   * Sets the data values found for a specific argument.
   *
   * @param arg         argument
   * @param paramValues parameter values for the argument
   */
  public void addResult(final Argument arg, final String[] paramValues) {
    List<String[]> list = values.get(arg);
    if (list == null) {
      list = new ArrayList<String[]>();
      list.add(paramValues);
      values.put(arg, list);
    } else {
      list.add(paramValues);
    }
  }

  /**
   * Returns a list of all defined arguments.
   *
   * @return set of arguments
   */
  public Set<Argument> getDefinedArguments() {
    return values.keySet();
  }

  /**
   * Counts the number of arguments defined on the command line which are in the
   * list provided.
   *
   * @param ofThese the arguments to search for, or null to count all supplied
   *                arguments
   * @return count of the defined arguments
   */
  public int getDefinedCount(Argument... ofThese) {
    if (ofThese.length == 0) {
      return values.keySet().size();
    }

    int count = 0;
    for (Argument arg : values.keySet()) {
      boolean found = false;
      for (int i = 0; !found && i < ofThese.length; i++) {
        if (ofThese[i].equals(arg)) {
          count++;
          found = true;
        }
      }
    }
    return count;
  }

  /**
   * Returns whetheror not the command line argument was actually provided on
   * the command line.
   *
   * @param arg argument to query
   * @return true if the argument is defined by the command line, false
   * otherwise
   */
  public boolean isDefined(final Argument arg) {
    final List<String[]> result = values.get(arg);
    return (result != null);
  }

  /**
   * Returns all results for the specified argument.  If a command line option
   * is specified more than once, this is the method to use to get all values.
   *
   * @param arg argument to query
   * @return list of all parameter lists defined for this argument
   */
  public List<String[]> getAllResults(final Argument arg) {
    List<String[]> result = values.get(arg);

    if (result == null) {
      final String[] envVars = arg.getEnvVars();
      final String[] defaults = arg.getDefaults();
      final String[] vals = new String[arg.getParameterCount()];
      boolean found = defaults != null;

      for (int i = 0; i < arg.getParameterCount(); i++) {
        if (defaults != null) {
          vals[i] = defaults[i];
        }
        if (envVars != null) {
          String val = System.getenv(envVars[i]);
          if (val != null) {
            found = true;
            vals[i] = val;
          }
        }
      }

      if (found) {
        result = new ArrayList<String[]>();
        result.add(vals);
      }
    }
    return result;
  }

  /**
   * Convenience method to retrieve the first instance of the command line
   * argument's values.
   *
   * @param arg argument to query
   * @return first parameter list defined for this argument
   */
  public String[] getResult(final Argument arg) {
    final List<String[]> all = getAllResults(arg);
    if (all == null) {
      return null;
    } else {
      return all.get(0);
    }
  }

  /**
   * Convenience method to return the first value of the first instance of the
   * command line argument values for the specified argument.
   *
   * @param arg argument to query
   * @return first parameter of the first list of parameters supplied
   */
  public String getFirstResult(final Argument arg) {
    final String[] all = getResult(arg);
    if (all == null) {
      return null;
    } else {
      return all[0];
    }
  }

  /**
   * Convenience method to return the result of getFirstResult method as an
   * integer.
   *
   * @param arg            argument to query
   * @param undefinedValue value to return when argument is not defined or is
   *                       illegally defined
   * @return value specified, or default value provided
   */
  public int getFirstResultAsInt(
      final Argument arg, final int undefinedValue) {
    final String value = getFirstResult(arg);
    if (value == null) {
      return undefinedValue;
    } else {
      return Integer.parseInt(value);
    }
  }

}
