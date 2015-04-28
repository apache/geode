/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * 
 * <h3>Description of the conditions under which this exception is thrown</h3>
 * <p>
 * When a <code>PartitionedRegionStorageException</code> message contains
 * the string:
 * 
 * <pre>
 * There are not enough data store members to create a bucket.
 * </pre>
 * 
 * A new data store must be added to the partitioned region for future bucket creation.
 * </p>
 * 
 * <p>
 * When a <code>PartitionedRegionStorageException</code> message contains
 * the string:
 * 
 * <pre>
 * Too many data store members have refused the request to create a bucket.
 * </pre>
 * 
 * There are enough data stores, however some have refused possibly due to these
 * conditions:
 * <ol>
 * <li>The amount of storage allocated to the partitioned region on that
 * distributed member exceeds its localMaxMemory setting.</li>
 * <li>The partitioned region instance on that distributed member has been
 * closed or destroyed.</li>
 * <li>The cache on that distributed member has been closed.</li>
 * <li>The distributed system on that member has been disconnected.</li>
 * </ol>
 * </p>
 * 
 * <p>
 * When a <code>PartitionedRegionStorageException</code> message contains
 * the string:
 * 
 * <pre>
 * Creation of a bucket for partitioned region failed in N attempts.
 * </pre>
 * If the number of attempts is lesser than the number of available
 * data store members, contact GemFire support providing all logs and statistics files from 
 * all members containing the partitioned region.  Otherwise, shutdown and then restart one 
 * or more of the data stores, given that it is safe to do so, for example when redundantCopies 
 * is greater than 0.
 * </p>
 * 
 * 
 * @see com.gemstone.gemfire.cache.PartitionAttributesFactory
 * @since 5.0
 */
public class PartitionedRegionStorageException extends CacheRuntimeException  {
private static final long serialVersionUID = 5905463619475329732L;

    /** Creates a new instance of PartitionedRegionStorageException */
    public PartitionedRegionStorageException() {
    }
    
    
    /**
     * Creates a new {@link PartitionedRegionStorageException} with a message.
     * @param msg The string message for the PartitionedRegionStorageException.
     */
 
    public PartitionedRegionStorageException(String msg) {
      super(msg);
    }    
    
    /**
     * Creates a new {@link PartitionedRegionStorageException} with a message and Throwable cause.
     * @param message The string message for the PartitionedRegionStorageException.
     * @param cause Throwable cause for this {@link PartitionedRegionStorageException}.
     */
    public PartitionedRegionStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
