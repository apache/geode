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
package org.apache.geode.internal.logging;

import static org.apache.commons.io.output.NullOutputStream.NULL_OUTPUT_STREAM;

import java.io.PrintStream;

public class NullLogWriter extends ManagerLogWriter {

  public NullLogWriter() {
    this(LogWriterLevel.NONE.intLevel(), new PrintStream(NULL_OUTPUT_STREAM), true);
  }

  public NullLogWriter(int level, PrintStream printStream, boolean loner) {
    super(level, printStream, loner);
  }

  public NullLogWriter(int level, PrintStream printStream, String connectionName, boolean loner) {
    super(level, printStream, connectionName, loner);
  }

  @Override
  public void configChanged() {
    // nothing
  }

  @Override
  public void startupComplete() {
    // nothing
  }

  @Override
  public void shuttingDown() {
    // nothing
  }

  @Override
  public void closingLogFile() {
    // nothing
  }
}
