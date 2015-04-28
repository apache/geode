/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReplicationReceiver.java,v 1.2 2005/07/17 11:36:40 chrislott Exp $


package com.gemstone.org.jgroups.blocks;


/**
 * Implementation of this interface needs to register with ReplicationManager and will receive updates to be
 * applied to its locally replicated data. If locks are used the implementation is resposible for lock acquisition
 * and management. To do so, it probably needs to maintain a lock table (keys = resource objects, values = transactions)
 * to associate resources with locks and possibly a transaction table (keys = transactions, values = locks) to keep
 * track of all locks for a given transaction (to commit/release all modifications/locks for a given transaction).
 *
 * @author Bela Ban Nov 19 2002
 */
public interface ReplicationReceiver {

    /**
     * Receives data sent by a sender to all group members and applies update to locally replicated data. This is 
     * the result of a {@link com.gemstone.org.jgroups.blocks.ReplicationManager#send} call.
     * 
     * @param transaction The transaction under which all locks will be acquired. Will be null if no locks are used (e.g.
     *                    <code>use_locks</code> is null).
     * @param data The data to be modified. In case of a database, this data would have to be stored in stable storage,
     *             and would only be applied on a <code>commit()</code>. In case of a distributed replicated in-memory
     *             data structure, the update might be applied directly and the subsequent commit() or rollback() might
     *             be ignored. Note that this argument may contain the resource to be locked; in this case the <code>
     *             lock_info</code> parameter might be null.
     * @param lock_info Information about the resource(s) to be locked. Will be null if no locks are used (e.g.
     *                  <code>use_locks</code> is null). Can also be null even if locks are used, e.g. when the resource(s)
     *                  to be locked are an implicit part of <code>data</code>.
     * @param lock_acquisition_timeout If locks are used, the number of milliseconds to wait for a lock to be acquired.
     *                                 If this time elapses, a TimeoutException will be thrown. A value of 0 means
     *                                 to wait forever. If <code>use_locks</code> is false, this value is ignored.
     * @param lock_lease_timeout The number of milliseconds to hold on to the lock, once it is acquired. A value of 0
     *                           means to never release the lock until commit() or rollback() are called.
     * @param use_locks Whether to use locking or not. If this value is false, all lock-related arguments will be
     *                  ignored, regardless of whether they are non-null.
     * @return Object A return value, the semantics of which are determined by caller of {@link com.gemstone.org.jgroups.blocks.ReplicationManager#send}
     *                and the receiver. If no special value should be returned, null can be returned. Note that in the
     *                latter case, null is still treated as a response (in the synchronous call).
     * @exception LockingException Thrown when a lock on a resource cannot be acquired
     * @exception UpdateException Thrown when the update fails (application semantics)
     */
    Object receive(Xid     transaction,
                   byte[]  data, 
                   byte[]  lock_info,
                   long    lock_acquisition_timeout,
                   long    lock_lease_timeout,
                   boolean use_locks) throws LockingException, UpdateException;


    /**
     * Commit the modifications to the locally replicated data and release all locks. If the receive() call already
     * applied the changes, then this method is a nop.
     */
    void commit(Xid transaction);


    /**
     * Discard all modifications and release all locks. If the receive() call already applied the changes,
     * this method will not be able to rollback the modifications, but will only release the locks.
     */
    void rollback(Xid transaction);
}
