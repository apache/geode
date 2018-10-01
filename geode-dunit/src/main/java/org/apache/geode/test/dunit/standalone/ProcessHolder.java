package org.apache.geode.test.dunit.standalone;


import java.io.InputStream;

//TODO:: Do we need ProcessHolder?
public class ProcessHolder {
  private final Process process;
  private volatile boolean killed = false;

  public ProcessHolder(Process process) {
    this.process = process;
  }

  public void kill() {
    this.killed = true;
    process.destroy();
  }

  public void killForcibly() {
    this.killed = true;
    process.destroyForcibly();
  }

  public void waitFor() throws InterruptedException {
    process.waitFor();
  }

  public InputStream getErrorStream() {
    return process.getErrorStream();
  }

  public InputStream getInputStream() {
    return process.getInputStream();
  }

  public boolean isKilled() {
    return killed;
  }

  public boolean isAlive() {
    return !killed && process.isAlive();
  }
}
