/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.controllers;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;

/**
 * For handling IO exception in our controllers
 * 
 * @author Riya Bhandekar
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
