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
package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

public class CliCommandException extends CliException {

  private CommandTarget commandTarget;
  private OptionSet optionSet;

  public CliCommandException(final CommandTarget commandTarget) {
    this(commandTarget, null, null);
  }

  public CliCommandException(final CommandTarget commandTarget, final OptionSet optionSet) {
    this(commandTarget, optionSet, null);
  }

  public CliCommandException(final CommandTarget commandTarget, final OptionSet optionSet, final Throwable cause) {
    super(cause);
    this.setCommandTarget(commandTarget);
    this.setOptionSet(optionSet);
  }

  public CommandTarget getCommandTarget() {
    return commandTarget;
  }

  /**
   * TODO: make this immutable
   *
   * @param commandTarget the commandTarget to set
   */
  public void setCommandTarget(CommandTarget commandTarget) {
    this.commandTarget = commandTarget;
  }

  public OptionSet getOptionSet() {
    return optionSet;
  }

  /**
   * TODO: make this immutable
   *
   * @param optionSet the optionSet to set
   */
  public void setOptionSet(OptionSet optionSet) {
    this.optionSet = optionSet;
  }
}
