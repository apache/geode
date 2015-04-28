/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin;

//import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when an administration operation that accesses information
 * in a remote system member is cancelled.  The cancelation may occur
 * because the system member has left the distributed system.
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class OperationCancelledException extends RuntimeAdminException {
   private static final long serialVersionUID = 5474068770227602546L;
    
    public OperationCancelledException() {
      super();
    }
    
    public OperationCancelledException( String message ) {
        super( message );
    }
    
    public OperationCancelledException( Throwable cause ){
      super(cause);
    }
    
    public OperationCancelledException( String message, Throwable cause ) {
      super(message, cause);
    }
}
