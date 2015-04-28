package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.distributed.AbstractLauncher.ServiceState;

/**
 * Defines the callbacks for handling stop and status by a {@link ControllableProcess}.
 * Separated from ControllableProcess so that an internal object can implement
 * this to avoid exposing these methods via the customer API.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public interface ControlNotificationHandler {
  public void handleStop();
  public ServiceState<?> handleStatus();
}
