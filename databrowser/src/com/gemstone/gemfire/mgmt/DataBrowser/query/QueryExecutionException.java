package com.gemstone.gemfire.mgmt.DataBrowser.query;


import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class QueryExecutionException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final String SERIALIZATION_EXCEPTION = "Failed to deserialize the Query results";
  private static final String QUERY_EXEC_EXCEPTION = "Failed to execute a Query";
  
  public QueryExecutionException() {
    super();
  }

  public QueryExecutionException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public QueryExecutionException(String arg0) {
    super(arg0);
  }

  public QueryExecutionException(Throwable arg0) {
    super(arg0);
  }
  
  public boolean isAuthenticationFailed(){
    boolean failed = false;
    Throwable cause = getCause();
    while(cause != null) {
      if(cause instanceof AuthenticationRequiredException || 
          cause instanceof AuthenticationFailedException ) {
        failed = true;
        break;
      }
        
      cause = cause.getCause();
    }
    return failed;
  }
  
  public boolean isAuthorizationFailed(){
    boolean failed = false;
    Throwable cause = getCause();
    while(cause != null) {
      if(cause instanceof NotAuthorizedException ) {
        failed = true;
        break;
      }
        
      cause = cause.getCause();
    }
    return failed;
  }
  

  @Override
  public String getMessage() {
    Throwable cause = getCause();
    boolean serverOp = (cause instanceof com.gemstone.gemfire.cache.client.ServerOperationException);
    
    if ( serverOp || (cause instanceof com.gemstone.gemfire.SerializationException)) {
     StringBuffer buffer = new StringBuffer();
      
      if(serverOp)
        buffer.append(QUERY_EXEC_EXCEPTION);
      else
        buffer.append(SERIALIZATION_EXCEPTION);
      
      buffer.append(" : ");
      buffer.append(cause);
      
      cause = cause.getCause();
      
      while(cause != null) {
        buffer.append('\n');
        buffer.append("Caused by ");
        buffer.append(cause);   
        cause = cause.getCause();
      }
      
      return buffer.toString();
    }
    
    return super.getMessage();
  }
  
  
  

}
