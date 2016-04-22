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

/**
 * Interface specifying the requirements for objects wiching to be able to
 * examine arguments (potentially tweaking parameters) at the time of parsing,
 * thereby allowing for usage display to occur automatically.
 */
public interface ArgumentHandler {

  /**
   * Process the argument values specified.
   *
   * @param arg    argument definition
   * @param form   form which was used on the command line
   * @param params parameters supplied to the argument
   * @throws UsageException when usage was suboptimal
   */
  void handleArgument(Argument arg, String form, String[] params)
      throws UsageException;

}
