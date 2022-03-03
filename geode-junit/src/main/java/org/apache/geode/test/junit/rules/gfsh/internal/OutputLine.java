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
package org.apache.geode.test.junit.rules.gfsh.internal;

public class OutputLine {
  private final String line;
  private final OutputSource source;

  private OutputLine(String line, OutputSource source) {
    this.line = line;
    this.source = source;
  }

  public static OutputLine fromStdErr(String line) {
    return new OutputLine(line, OutputSource.STD_ERR);
  }

  public static OutputLine fromStdOut(String line) {
    return new OutputLine(line, OutputSource.STD_OUT);
  }

  public String getLine() {
    return line;
  }

  public OutputSource getSource() {
    return source;
  }

  public enum OutputSource {
    STD_ERR, STD_OUT
  }
}
