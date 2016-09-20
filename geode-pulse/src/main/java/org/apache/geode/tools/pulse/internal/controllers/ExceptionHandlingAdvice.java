/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.controllers;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;

/**
 * For handling IO exception in our controllers
 * 
 * 
 */
@ControllerAdvice
public class ExceptionHandlingAdvice {

  @ExceptionHandler(IOException.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public void handleExc(IOException ext) {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    // write errors
    StringWriter swBuffer = new StringWriter();
    PrintWriter prtWriter = new PrintWriter(swBuffer);
    ext.printStackTrace(prtWriter);
    LOGGER.severe("IOException Details : " + swBuffer.toString() + "\n");
  }
}
