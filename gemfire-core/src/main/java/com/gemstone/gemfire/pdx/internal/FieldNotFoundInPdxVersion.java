package com.gemstone.gemfire.pdx.internal;

public class FieldNotFoundInPdxVersion extends Exception{

  private static final long serialVersionUID = 1292033563588485577L;

  public FieldNotFoundInPdxVersion(String message) {
    super(message);
  }
  
}
