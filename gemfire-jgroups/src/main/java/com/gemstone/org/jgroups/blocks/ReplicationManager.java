/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReplicationManager.java,v 1.7 2004/09/23 16:29:11 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.RspList;

import java.io.Serializable;





/**
 * Class to propagate updates to a number of nodes in various ways:
 * <ol>
 * <li>Asynchronous
 * <li>Synchronous
 * <li>Synchronous with locking
 * </ol>
 * 
 * <br/><em>Note: This class is experimental as of Oct 2002</em>
 *
 * @author Bela Ban Oct 2002
 */
public class ReplicationManager implements RequestHandler {
    Address                     local_addr=null;
    ReplicationReceiver         receiver=null;

    /** Used to broadcast updates and receive responses (latter only in synchronous case) */
    protected MessageDispatcher disp=null;

    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());



    /**
     * Creates an instance of ReplicationManager on top of a Channel
     */
    public ReplicationManager(Channel channel,
                              MessageListener ml,
                              MembershipListener l,
                              ReplicationReceiver receiver) {
        setReplicationReceiver(receiver);
	if(channel != null) {
            local_addr=channel.getLocalAddress();
            disp=new MessageDispatcher(channel,
                                       ml,
                                       l,
                                       this,      // ReplicationManager is RequestHandler
                                       true);     // use deadlock detection
        }
    }
    

    /**
     * Creates an instance of ReplicationManager on top of a PullPushAdapter
     */
    public ReplicationManager(PullPushAdapter adapter,
                              Serializable id,
                              MessageListener ml,
                              MembershipListener l,
                              ReplicationReceiver receiver) {
        if(adapter != null && adapter.getTransport() != null && adapter.getTransport() instanceof Channel)
            local_addr=((Channel)adapter.getTransport()).getLocalAddress();
        setReplicationReceiver(receiver);
        disp=new MessageDispatcher(adapter,
                                   id,     // FIXME
                                   ml,
                                   l,
                                   this);  // ReplicationManager is RequestHandler
        disp.setDeadlockDetection(true);
    }
    


    public void stop() {
        if(disp != null)
            disp.stop();
    }
    


    /**
     * Create a new transaction. The transaction will be used to send updates, identify updates in the same transaction,
     * and eventually commit or rollback the changes associated with the transaction.
     * @return Xid A unique transaction
     * @exception Exception Thrown when local_addr is null
     */
    public Xid begin() throws Exception {
        return begin(Xid.DIRTY_READS);
    }


    /**
     * Create a new transaction. The tracsion will be used to send updates, identify updates in the same transaction,
     * and eventually commit or rollback the changes associated with the transaction.
     * @param transaction_mode Mode in which the transaction should run. Possible values are Xid.DIRTY_READS,
     *                         Xid.READ_COMMITTED, Xid.REPEATABLE_READ and Xid.SERIALIZABLE
     * @return Xid A unique transaction
     * @exception Exception Thrown when local_addr is null
     */
    public Xid begin(int transaction_mode) throws Exception {
        return Xid.create(local_addr, transaction_mode);
    }

    
    public void setReplicationReceiver(ReplicationReceiver handler) {
        this.receiver=handler;
    }

    public void setMembershipListener(MembershipListener l) {
        if(l == null)
            return;
        if(disp == null) { // GemStoneAddition: missing braces
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_DISPATCHER_IS_NULL_CANNOT_SET_MEMBERSHIPLISTENER);
        }
        else
            disp.setMembershipListener(l);
    }
    

    /**
     * Sends a request to all members of the group. Sending is asynchronous (return immediately) or
     * synchronous (wait for all members to respond). If <code>use_locking</code> is true, then locking
     * will be used at the receivers to acquire locks before accessing/updating a resource. Locks can be
     * explicitly set using <code>lock_info</code> or implicitly through <code>data</code>. In the latter
     * case, locks are induced from the data sent, e.g. if the data is a request for updating a certain row
     * in a table, then we need to acquire a lock for that table.<p>
     * In case of using locks, if the transaction associated with update already has a lock for a given resource,
     * we will return. Otherwise, we will wait for <code>lock_acquisition_timeout</code> milliseconds. If the lock
     * is not granted within that time a <code>LockingException</code> will be thrown. (<em>We hope to
     * replace this timeout with a distributed deadlock detection algorithm in the future.</em>)<p>
     * We have 3 main use case for this method:
     * <ol>
     * <li><b>Asynchronous</b>: sends the message and returns immediately. Argument <code>asynchronous</code>
     *     needs to be true. All other arguments except <code>data</code> are ignored and can be null. Will call
     *     <code>update()</code> on the registered ReplicationReceiver at each receiver.
     * <li><b>Synchronous without locks</b>: sends the message, but returns only after responses from all members
     *     have been received, or <code>synchronous_timeout</code> milliseconds have elapsed (whichever comes
     *     first). Argument <code>asynchronous</code> needs to be false. Argument <code>synchronous_timeout</code>
     *     needs to be >= 0. If it is null the call will not time out, but wait for all responses.
     *     All other arguments (besides <code>data</code> are ignored).
     * <li><b>Synchronous with locks</b>: sends the message, but returns only after responses from all members
     *     have been received, or <code>synchronous_timeout</code> milliseconds have elapsed (whichever comes
     *     first). At the receiver's side we have to acquire a lock for the resource to be updated, if the 
     *     acquisition fails a LockingException will be thrown. The resource to be locked can be found in two ways:
     *     either <code>data</code> contains the resource(c) to be acquired implicitly, or <code>lock_info</code>
     *     lists the resources explicitly, or both. All the locks acquired at the receiver's side should be associated
     *     with <code>transaction</code>. When a <code>commit()</code> is received, the receiver should commit
     *     the modifications to the resource and release all locks. When a <code>rollback()</code> is received,
     *     the receiver should remove all (temporary) modifications and release all locks associated with
     *     <code>transaction</code>.
     * </ol>
     * In both the synchronous cases a List of byte[] will be returned if the data was sent to all receivers
     * successfully, cointaining byte buffers. The list may be empty.
     * @param dest The destination to which to send the message. Will be sent to all members if null.
     * @param data The data to be sent to all members. It may contain information about the resource to be locked.
     * @param synchronous If false the call is asynchronous, ie. non-blocking. If true, the method will wait
     *                    until responses from all members have been received (unless a timeout is defined, see below)
     * @param synchronous_timeout In a synchronous call, we will wait for responses from all members or until
     *                            <code>synchronous_timeout</code> have elapsed (whichever comes first). 0 means
     *                            to wait forever.
     * @param transaction The transaction under which all locks for resources should be acquired. The receiver
     *                    will probably maintain a lock table with resources as keys and transactions as values.
     *                    When an update is received, the receiver checks its lock table: if the resource is
     *                    not yet taken, the resource/transaction pair will be added to the lock table. Otherwise,
     *                    we check if the transaction's owner associated with the resource is the same as the caller.
     *                    If this is the case, the lock will be considered granted, otherwise we will wait for the
     *                    resource to become available (for a certain amount of time). When a transaction is 
     *                    committed or rolled back, all resources associated with this transaction will be released.
     * @param lock_info Information about resource(s) to be acquired. This may be null, e.g. if this information
     *                  is already implied in <code>data</code>. Both <code>data</code> and <code>lock_info</code>
     *                  may be used to define the set of resources to be acquired.
     * @param lock_acquisition_timeout The number of milliseconds to wait until a lock acquisition request is
     *                                 considered failed (causing a LockingException). If 0 we will wait forever.
     *                                 (Note that this may lead to deadlocks).
     * @param lock_lease_timeout The number of milliseconds we want to keep the lock for a resource. After
     *                           this time has elapsed, the lock will be released. If 0 we won't release the lock(s)
     * @param use_locks If this is false, we will ignore all lock information (even if it is specified) and
     *                  not use locks at all.
     * @return RspList A list of Rsps ({@link com.gemstone.org.jgroups.util.Rsp}), one for each member. Each one is the result of
     *                 {@link ReplicationReceiver#receive}. If a member didn't send a response, the <code>received</code>
     *                 field will be false. If the member was suspected while waiting for a response, the <code>
     *                 suspected</code> field will be true. If the <code>receive()</code> method in the receiver returned
     *                 a value it will be in field <code>retval</code>. If the receiver threw an exception it will also
     *                 be in this field.
     */
    public RspList send(Address dest,
                        byte[]  data,
                        boolean synchronous,
                        long    synchronous_timeout,
                        Xid     transaction,
                        byte[]  lock_info,
                        long    lock_acquisition_timeout,
                        long    lock_lease_timeout,
                        boolean use_locks) { // throws UpdateException, TimeoutException, LockingException {
        
        Message         msg=null;
        ReplicationData d=new ReplicationData(ReplicationData.SEND,
                                              data,
                                              transaction,
                                              lock_info,
                                              lock_acquisition_timeout,
                                              lock_lease_timeout,
                                              use_locks);

            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicationManager_DATA_IS__0__SYNCHRONOUS_1, new Object[] {d, Boolean.valueOf(synchronous)});
        msg=new Message(dest, null, d);
        if(synchronous) {
            return disp.castMessage(null, msg, GroupRequest.GET_ALL, synchronous_timeout);
        }
        else {
            disp.castMessage(null, msg, GroupRequest.GET_NONE, 0);
            return null;
        }
    }

    
    /**
     * Commits all modifications sent to the receivers via {@link #send} and releases all locks associated with
     * this transaction. If modifications were made to stable storage (but not to resource), those modifications
     * would now need to be transferred to the resource (e.g. database).
     */
    public void commit(Xid transaction) {
        sendMessage(ReplicationData.COMMIT, transaction);
    }


    /**
     * Discards all modifications sent to the receivers via {@link #send} and releases all locks associated with
     * this transaction.
     */
    public void rollback(Xid transaction) {
        sendMessage(ReplicationData.ROLLBACK, transaction);
    }


    /* ------------------------------- RequestHandler interface ------------------------------ */

    public Object handle(Message msg) {
        Object          retval=null;
        ReplicationData data;

        if(msg == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_RECEIVED_MESSAGE_WAS_NULL);
            return null;
        }

        if(msg.getLength() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_PAYLOAD_OF_RECEIVED_MESSAGE_WAS_NULL);
            return null;
        }
        
        try {
            data=(ReplicationData)msg.getObject();
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_FAILURE_UNMARSHALLING_MESSAGE__0, ex);
            return null;
        }

        switch(data.getType()) {
        case ReplicationData.SEND:
            try {
                return handleSend(data);
            }
            catch(Throwable ex) {
              if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_FAILED_HANDLING_UPDATE__0, ex);
              return ex;
            }
        case ReplicationData.COMMIT:
            handleCommit(data.getTransaction());
            break;
        case ReplicationData.ROLLBACK:
            handleRollback(data.getTransaction());
            break;
        default:
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicationManager_RECEIVED_INCORRECT_REPLICATION_MESSAGE__0, data);
            return null;
        }

        return retval;
    }
    
    /* --------------------------- End of RequestHandler interface---------------------------- */


    protected Object handleSend(ReplicationData data) throws UpdateException, LockingException {
        try {
            if(receiver == null) {
                if(log.isWarnEnabled()) log.warn("receiver is not set");
                return null;
            }
            return receiver.receive(data.getTransaction(),
                                    data.getData(),
                                    data.getLockInfo(),
                                    data.getLockAcquisitionTimeout(),
                                    data.getLockLeaseTimeout(),
                                    data.useLocks());
        }
        catch(Throwable ex) {
            return ex;            
        }
    }


    protected void handleCommit(Xid transaction) {
        if(receiver == null) {
            if(log.isWarnEnabled()) log.warn("receiver is not set");
        }
        else
            receiver.commit(transaction);
    }

    protected void handleRollback(Xid transaction) {
        if(receiver == null) {
            if(log.isWarnEnabled()) log.warn("receiver is not set");
        }
        else
            receiver.rollback(transaction);
    }




    /* -------------------------------------- Private methods ------------------------------------ */

    
    void sendMessage(int type, Xid transaction) {
        ReplicationData data=new ReplicationData(type, null, transaction, null, 0, 0, false);
        Message msg=new Message(null, null, data);
        disp.castMessage(null, msg, GroupRequest.GET_NONE, 0); // send commit message asynchronously
    }


    /* ---------------------------------- End of Private methods --------------------------------- */
	
}
