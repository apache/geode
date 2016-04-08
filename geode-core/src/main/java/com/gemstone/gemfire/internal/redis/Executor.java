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
package com.gemstone.gemfire.internal.redis;


/**
 * Interface for executors of a {@link Command}
 * 
 *
 */
public interface Executor {

  /**
   * This method executes the command and sets the response. Any runtime errors
   * from this execution should be handled by caller to ensure the client gets 
   * a response
   * 
   * @param command The command to be executed
   * @param context The execution context by which this command is to be executed
   */
  public void executeCommand(Command command, ExecutionHandlerContext context);
  
}
