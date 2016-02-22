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
package com.gemstone.gemfire.internal.io;

import java.io.PrintStream;

/**
 * @since 7.0
 */
public class TeePrintStream extends PrintStream {

  private final TeeOutputStream teeOut;
  
  public TeePrintStream(TeeOutputStream teeOut) {
    super(teeOut, true);
    this.teeOut = teeOut;
  }
  
  public TeeOutputStream getTeeOutputStream() {
    return this.teeOut;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("teeOutputStream=").append(this.teeOut);
    return sb.append("}").toString();
  }
}
