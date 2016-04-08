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
package com.gemstone.gemfire.internal.jta;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.PureLogWriter;

/**
 * Contains Utility functions for use by JTA
 * 
 */
public class TransactionUtils {

  private static LogWriterI18n dslogWriter = null;
  private static LogWriterI18n purelogWriter = null;

  /**
   * Returns the logWriter associated with the existing DistributedSystem. If
   * DS is null then the PureLogWriter is returned
   * 
   * @return LogWriterI18n
   */
  public static LogWriterI18n getLogWriterI18n() {
    if (dslogWriter != null) {
      return dslogWriter;
    } else if (purelogWriter != null) {
      return purelogWriter;
    } else {
      purelogWriter = new PureLogWriter(InternalLogWriter.SEVERE_LEVEL);
      return purelogWriter;
    }
  }

  /**
   * To be used by mapTransaction method of JNDIInvoker to set the dsLogwriter
   * before the binding of the datasources
   * 
   * @param logWriter
   */
  public static void setLogWriter(LogWriterI18n logWriter) {
    dslogWriter = logWriter;
  }
}
