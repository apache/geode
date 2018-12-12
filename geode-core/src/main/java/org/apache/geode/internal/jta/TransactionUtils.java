/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.jta;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.PureLogWriter;

/**
 * Contains Utility functions for use by JTA
 *
 */
public class TransactionUtils {

  private static LogWriter dslogWriter = null;
  private static LogWriter purelogWriter = null;

  /**
   * Returns the logWriter associated with the existing DistributedSystem. If DS is null then the
   * PureLogWriter is returned
   *
   */
  public static LogWriter getLogWriter() {
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
   * To be used by mapTransaction method of JNDIInvoker to set the dsLogwriter before the binding of
   * the datasources
   *
   */
  public static void setLogWriter(LogWriter logWriter) {
    dslogWriter = logWriter;
  }
}
