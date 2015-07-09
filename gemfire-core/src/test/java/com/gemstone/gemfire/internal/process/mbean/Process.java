package com.gemstone.gemfire.internal.process.mbean;

/**
 * Extracted from LocalProcessControllerDUnitTest.
 * 
 * @author Kirk Lund
 */
public class Process implements ProcessMBean {
  
  private final Object object = new Object();
  private final int pid;
  private final boolean process;
  private volatile boolean stop;
  
  public Process(int pid, boolean process) {
    this.pid = pid;
    this.process = process;
  }
  
  @Override
  public int getPid() {
    return this.pid;
  }
  
  public boolean isProcess() {
    return this.process;
  }
  
  @Override
  public void stop() {
    synchronized (this.object) {
      this.stop = true;
      this.object.notifyAll();
    }
  }
  
  public void waitUntilStopped() throws InterruptedException {
    synchronized (this.object) {
      while (!this.stop) {
        this.object.wait();
      }
    }
  }
}
