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
package com.gemstone.gemfire.internal.process;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Defines the operations for controlling a running process.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public interface ProcessController {

  /**
   * Returns the status of a running GemFire {@link ControllableProcess}.
   */
  public String status() throws UnableToControlProcessException, ConnectionFailedException, IOException, MBeanInvocationFailedException, InterruptedException, TimeoutException;
  
  /**
   * Stops a running GemFire {@link ControllableProcess}.
   */
  public void stop() throws UnableToControlProcessException, ConnectionFailedException, IOException, MBeanInvocationFailedException;
  
  /**
   * Returns the PID of a running GemFire {@link ControllableProcess}.
   */
  public int getProcessId();

  /**
   * Checks if {@link #status} and {@link #stop} are supported if only the PID 
   * is provided. Only the {@link MBeanProcessController} supports the use of
   * specifying a PID because it uses the Attach API.
   *  
   * @throws com.gemstone.gemfire.lang.AttachAPINotFoundException if the Attach API is not found
   */
  public void checkPidSupport();
  
  /**
   * Defines the arguments that a client must provide to the ProcessController.
   */
  static interface Arguments {
    public int getProcessId();
    public ProcessType getProcessType();
  }
}
