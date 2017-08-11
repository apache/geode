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

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notEmpty;
import static org.apache.commons.lang.Validate.notNull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Manages which implementation of {@link ProcessController} will be used and constructs the
 * instance.
 * 
 * @since GemFire 8.0
 */
public class ProcessControllerFactory {

  /**
   * For testing only
   */
  public static final String PROPERTY_DISABLE_ATTACH_API =
      DistributionConfig.GEMFIRE_PREFIX + "test.ProcessControllerFactory.DisableAttachApi";

  private final boolean disableAttachApi;

  public ProcessControllerFactory() {
    this.disableAttachApi = Boolean.getBoolean(PROPERTY_DISABLE_ATTACH_API);
  }

  public ProcessController createProcessController(final ProcessControllerParameters parameters,
      final int pid) {
    notNull(parameters, "Invalid parameters '" + parameters + "' specified");
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

    if (isAttachAPIFound()) {
      try {
        return new MBeanProcessController(parameters, pid);
      } catch (ExceptionInInitializerError ignore) {
      }
    }
    return new FileProcessController(parameters, pid);
  }

  public ProcessController createProcessController(final ProcessControllerParameters parameters,
      final File directory, final String pidFileName)
      throws IOException, InterruptedException, TimeoutException {
    notNull(parameters, "Invalid parameters '" + parameters + "' specified");
    notNull(directory, "Invalid directory '" + directory + "' specified");
    notEmpty(pidFileName, "Invalid pidFileName '" + pidFileName + "' specified");

    return createProcessController(parameters, readPid(directory, pidFileName));
  }

  public boolean isAttachAPIFound() {
    if (disableAttachApi) {
      return false;
    }
    boolean found = false;
    try {
      final Class<?> virtualMachineClass = Class.forName("com.sun.tools.attach.VirtualMachine");
      found = virtualMachineClass != null;
    } catch (ClassNotFoundException ignore) {
    }
    return found;
  }

  private int readPid(final File directory, final String pidFileName) throws IOException {
    return new PidFile(directory, pidFileName).readPid();
  }
}
