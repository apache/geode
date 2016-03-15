package com.gemstone.gemfire.test.dunit;


public class NamedRunnable implements SerializableRunnableIF {

  private static final long serialVersionUID = -2786841298145567914L;

  String name;
  SerializableRunnableIF delegate;
  
  public NamedRunnable(String name, SerializableRunnableIF delegate) {
    this.name = name;
    this.delegate = delegate;
  }
  
  @Override
  public void run() throws Exception {
    delegate.run();
  }
  
  @Override
  public String toString() {
    return ("runnable("+name+")");
  }

}
