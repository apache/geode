package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.internal.process.ProcessUtils.InternalProcessUtils;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

/**
 * Implementation of the {@link ProcessUtils} SPI that uses the JDK Attach API.
 * 
 * @author Kirk Lund
 * @since 8.0
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
}
