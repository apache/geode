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

/**
 * @author njadhav
 * 
 *         Converts joptsimple exceptions into corresponding exceptions for cli
 * 
 */
public class ExceptionGenerator {

  public static CliException generate(OptionException oe) {
    if (oe instanceof MissingRequiredOptionException) {
      return new CliCommandOptionMissingException(null, null, null);
    } else if (oe instanceof OptionMissingRequiredArgumentException) {
      return new CliCommandOptionValueMissingException(null, null, null, null);
    } else if (oe instanceof UnrecognizedOptionException) {
      return new CliCommandOptionNotApplicableException(null, null, null);
    } else if (oe instanceof MultipleArgumentsForOptionException) {
      return new CliCommandOptionHasMultipleValuesException(null, null, null);
    } else {
      return null;
    }
  }
}
