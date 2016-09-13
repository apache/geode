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
 * 
 * @since GemFire 7.0
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
