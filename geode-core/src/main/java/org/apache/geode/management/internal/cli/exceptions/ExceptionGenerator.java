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

import joptsimple.OptionException;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

/**
 * Converts joptsimple exceptions into corresponding exceptions for cli
 *
 * TODO: delete this class
 */
public class ExceptionGenerator {

  public static CliCommandOptionException generate(Option option, OptionException cause) {
    if (cause.getClass().getSimpleName().contains("MissingRequiredOptionException")) {
      return new CliCommandOptionMissingException(option, cause);

    } else if (cause.getClass().getSimpleName().contains("OptionMissingRequiredArgumentException")) {
      return new CliCommandOptionValueMissingException(option, cause);

    } else if (cause.getClass().getSimpleName().contains("UnrecognizedOptionException")) {
      return new CliCommandOptionNotApplicableException(option, cause);

    } else if (cause.getClass().getSimpleName().contains("MultipleArgumentsForOptionException")) {
      return new CliCommandOptionHasMultipleValuesException(option, cause);

    } else {
      return new CliCommandOptionException(cause);
    }
  }
}
