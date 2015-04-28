package com.gemstone.gemfire;

public class InvalidVersionException extends GemFireException {
  private static final long serialVersionUID = 6342040462194322698L;

  public InvalidVersionException(String msg) {
    super(msg);
  }
}
