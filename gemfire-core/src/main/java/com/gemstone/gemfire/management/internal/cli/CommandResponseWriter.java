/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.io.PrintWriter;
import java.io.StringWriter;


/**
 * Some methods decorated to be commands may have return type as 'void'. For
 * such commands, the result.response can be written in the
 * CommandResponseWriter. Each command execution will have a ThreadLocal copy of
 * this writer which will be accessible through
 * {@link com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext#WRITER_WRAPPER}. 
 * NOTE: Not thread safe 
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class CommandResponseWriter {
  private PrintWriter pwriter;
  private StringWriter swriter;
  
  public CommandResponseWriter() {
    swriter = new StringWriter();
    pwriter = new PrintWriter(swriter, true);
  }

  public CommandResponseWriter print(Object object) {
    pwriter.print(object);
    return this;
  }
  
  public CommandResponseWriter println(Object object) {
    pwriter.println(object);
    return this;
  }
  
  /**
   * @see PrintWriter#printf(String, Object...)
   */
  public CommandResponseWriter printf(String format, Object... args) {
    pwriter.printf(format, args);
    return this;
  }
  
  public String getResponseWritten() {
    return swriter.toString();
  }
  
}
