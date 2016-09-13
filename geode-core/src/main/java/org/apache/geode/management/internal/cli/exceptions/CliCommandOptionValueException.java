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
package org.apache.geode.management.internal.cli.exceptions;

import org.apache.geode.management.internal.cli.parser.CommandTarget;
import org.apache.geode.management.internal.cli.parser.Option;
import org.apache.geode.management.internal.cli.parser.OptionSet;

public class CliCommandOptionValueException extends CliCommandOptionException {

  private final String value;

  public CliCommandOptionValueException(final CommandTarget commandTarget, final Option option, final String value) {
    this(commandTarget, option, null, value, null);
  }

  public CliCommandOptionValueException(final CommandTarget commandTarget, final Option option, final OptionSet optionSet, final String value) {
    this(commandTarget, option, null, value, null);
  }

  public CliCommandOptionValueException(final Throwable cause) {
    this(null, null, null, null, cause);
  }

  public CliCommandOptionValueException(final CommandTarget commandTarget, final Option option, final OptionSet optionSet, final String value, final Throwable cause) {
    super(commandTarget, option, optionSet, cause);
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
