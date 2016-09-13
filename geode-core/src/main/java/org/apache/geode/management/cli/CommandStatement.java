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
package com.gemstone.gemfire.management.cli;

import java.util.Map;

/**
 * Represents GemFire Command Line Interface (CLI) command strings. A
 * <code>CommandStatement</code> instance can be used multiple times to process
 * the same command string repeatedly.
 * 
 * 
 * @since GemFire 7.0
 */
public interface CommandStatement {
  
  /**
   * Returns the user specified command string.
   */
  public String getCommandString();
  
  /**
   * Returns the CLI environment variables.
   */
  public Map<String, String> getEnv();
  
  /**
   * Processes this command statement with the user specified command string
   * and environment
   * 
   * @return The {@link Result} of the execution of this command statement.
   */
  public Result process();
  
  
  /**
   * Returns whether the command statement is well formed.
   * 
   * @return True if the command statement is well formed, false otherwise.
   */
  public boolean validate();
}
