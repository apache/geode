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
package org.apache.geode.internal.process;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Process controller that attempts to use the MBeanProcessController but
 * falls back on FileProcessController if that fails
 */
public class MBeanOrFileProcessController implements ProcessController {
  private final ProcessController mbeanController;
  private final ProcessController fileController;

  public MBeanOrFileProcessController(ProcessControllerParameters parameters, int pid) {
    mbeanController = new MBeanProcessController(parameters, pid);
    fileController = new FileProcessController(parameters, pid);
  }

  @Override
  public String status() throws UnableToControlProcessException, ConnectionFailedException,
      IOException, MBeanInvocationFailedException, InterruptedException, TimeoutException {
    try {
      return mbeanController.status();
    } catch (IOException | MBeanInvocationFailedException | ConnectionFailedException
        | UnableToControlProcessException e) {
      // if mbeanController already determines no such process exists, we can skip trying with
      // fileController
      String message = e.getMessage();
      if (message != null && message.toLowerCase().contains("no such process")) {
        throw e;
      }
      return fileController.status();
    }
  }

  @Override
  public void stop() throws UnableToControlProcessException, ConnectionFailedException, IOException,
      MBeanInvocationFailedException {

    try {
      mbeanController.stop();
    } catch (IOException | MBeanInvocationFailedException | ConnectionFailedException
        | UnableToControlProcessException e) {
      fileController.stop();
    }

  }

  @Override
  public int getProcessId() {
    return mbeanController.getProcessId();
  }

  @Override
  public void checkPidSupport() {}
}
