package com.gemstone.gemfire.management.internal.security;

public class AccessControlContext extends ResourceOperationContext {
  
  private ResourceOperationCode code;
  
  public AccessControlContext(String code){
    this.code = ResourceOperationCode.parse(code);
  }

  @Override
  public ResourceOperationCode getResourceOperationCode() {
    return code;
  }

  @Override
  public OperationCode getOperationCode() {   
    return OperationCode.RESOURCE;
  }  

}
