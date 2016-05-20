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

import joptsimple.MissingRequiredOptionException;
import joptsimple.MultipleArgumentsForOptionException;
import joptsimple.OptionException;
import joptsimple.OptionMissingRequiredArgumentException;
import joptsimple.UnrecognizedOptionException;

import com.gemstone.gemfire.management.internal.cli.parser.Option;

/**
 * Converts joptsimple exceptions into corresponding exceptions for cli
 */
public class ExceptionGenerator {

  public static CliCommandOptionException generate(Option option, OptionException e) {
    if (MissingRequiredOptionException.class.isInstance(e)) {
      return new CliCommandOptionMissingException(e);

    } else if (OptionMissingRequiredArgumentException.class.isInstance(e)) {
      return new CliCommandOptionValueMissingException(e);

    } else if (UnrecognizedOptionException.class.isInstance(e)) {
      return new CliCommandOptionNotApplicableException(e);

    } else if (MultipleArgumentsForOptionException.class.isInstance(e)) {
      return new CliCommandOptionHasMultipleValuesException(e);

    } else {
      return null;
    }
  }
}
