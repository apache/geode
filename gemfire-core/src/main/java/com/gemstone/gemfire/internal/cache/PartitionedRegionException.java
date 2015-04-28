/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheRuntimeException;

/**
 * RuntimeException propagated to invoking code to signal problem with remote
 * operations.
 *
 */
public class PartitionedRegionException extends CacheRuntimeException  {
private static final long serialVersionUID = 5113786059279106007L;

    /** Creates a new instance of ParititonedRegionException */
    public PartitionedRegionException() {
    }
    
    /** Creates a new instance of PartitionedRegionException 
     *@param msg 
     */
    public PartitionedRegionException(String msg) {
      super(msg);
    }    

    //////////////////////  Constructors  //////////////////////

    /**
     * Creates a new <code>PartitionedRegionException</code>.
     */
    public PartitionedRegionException(String message, Throwable cause) {
        super(message, cause);
    }
}
