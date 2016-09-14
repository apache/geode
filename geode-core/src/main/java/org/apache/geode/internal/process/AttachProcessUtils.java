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
package org.apache.geode.internal.process;

import org.apache.geode.internal.process.ProcessUtils.InternalProcessUtils;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

/**
 * Implementation of the {@link ProcessUtils} SPI that uses the JDK Attach API.
 * 
 * @since GemFire 8.0
 */
final class AttachProcessUtils implements InternalProcessUtils {
  
  AttachProcessUtils() {}
  
  @Override
  public boolean isProcessAlive(final int pid) {
    for (VirtualMachineDescriptor vm : VirtualMachine.list()) {
      if (vm.id().equals(String.valueOf(pid))) {
        return true; // found the vm
      }
    }
    return false;
  }

  @Override
  public boolean killProcess(final int pid) {
    throw new UnsupportedOperationException("killProcess(int) not supported by AttachProcessUtils");
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public boolean isAttachApiAvailable() {
    return true;
  }
}
