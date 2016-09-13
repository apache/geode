/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache;

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
 * @see org.apache.geode.cache.PartitionAttributesFactory
 * @since GemFire 5.0
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
