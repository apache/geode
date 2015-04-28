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
   * @throws AttachAPINotFoundException if the Attach API is not found
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
