package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.internal.process.ProcessUtils.InternalProcessUtils;
import com.gemstone.gemfire.internal.shared.NativeCalls;

/**
 * Implementation of the {@link ProcessUtils} SPI that uses {@link NativeCalls}.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
final class NativeProcessUtils implements InternalProcessUtils {
  
  private final static NativeCalls nativeCalls = NativeCalls.getInstance();
  
  NativeProcessUtils() {}
  
  @Override
  public boolean isProcessAlive(int pid) {
    return nativeCalls.isProcessActive(pid);
  }
  
  @Override
  public boolean killProcess(int pid) {
    return nativeCalls.killProcess(pid);
  }
}
