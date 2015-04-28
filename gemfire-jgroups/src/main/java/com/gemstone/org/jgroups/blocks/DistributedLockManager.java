/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.blocks.VotingAdapter.FailureVoteResult;
import com.gemstone.org.jgroups.blocks.VotingAdapter.VoteResult;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Rsp;
import com.gemstone.org.jgroups.util.RspList;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Distributed lock manager is responsible for maintaining the lock information
 * consistent on all participating nodes.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: DistributedLockManager.java,v 1.6 2005/06/08 15:56:54 publicnmi Exp $
 */
public class DistributedLockManager implements TwoPhaseVotingListener, LockManager, VoteResponseProcessor {
    /**
     * Definitions for the implementation of the VoteResponseProcessor
     */
    private static final int PROCESS_CONTINUE = 0;
    private static final int PROCESS_SKIP = 1;
    private static final int PROCESS_BREAK = 2;

    /**
     * This parameter means that lock acquisition expires after 5 seconds.
     * If there were no "commit" operation on prepared lock, then it
     * is treated as expired and is removed from the prepared locks table.
     */
    private static final long ACQUIRE_EXPIRATION = 5000;
    
    /**
     * This parameter is used during lock releasing. If group fails to release
     * the lock during the specified period of time, unlocking fails.
     */
    private static final long VOTE_TIMEOUT = 10000;

	// list of all prepared locks
	private final HashMap preparedLocks = new HashMap();

	// list of all prepared releases
	private final HashMap preparedReleases = new HashMap();

	// list of locks on the node
	private final HashMap heldLocks = new HashMap();

	private final TwoPhaseVotingAdapter votingAdapter;

	private final Object id;

    protected final GemFireTracer log=GemFireTracer.getLog(getClass());

    // @todo check if the node with the same id is already in the group.
    /**
     * Create instance of this class.
     * 
     * @param voteChannel instance of {@link VotingAdapter} that will be used 
     * for voting purposes on the lock decrees. <tt>voteChannel()</tt> will
     * be wrapped by the instance of the {@link TwoPhaseVotingAdapter}.
     * 
     * @param id the unique identifier of this lock manager.
     */
    public DistributedLockManager(VotingAdapter voteChannel, Object id) {
        this(new TwoPhaseVotingAdapter(voteChannel), id);
    }

    // @todo check if the node with the same id is already in the group.
    /**
     *  Constructor for the DistributedLockManager_cl object.
     * 
     *  @param channel instance of {@link TwoPhaseVotingAdapter}
     *  that will be used for voting purposes on the lock decrees.
     * 
     *  @param id the unique identifier of this lock manager.
     */
    public DistributedLockManager(TwoPhaseVotingAdapter channel, Object id) {
        this.id = id;
        this.votingAdapter = channel;
        this.votingAdapter.addListener(this);
    }

    /**
     * Performs local lock. This method also performs the clean-up of the lock
     * table, all expired locks are removed.
     */
    private boolean localLock(LockDecree lockDecree) {
        // remove expired locks
        removeExpired(lockDecree);

        LockDecree localLock =
            (LockDecree) heldLocks.get(lockDecree.getKey());

        if (localLock == null) {

            // promote lock into commited state
            lockDecree.commit();

            // no lock exist, perform local lock, note:
            // we do not store locks that were requested by other manager.
            if (lockDecree.managerId.equals(id))
                heldLocks.put(lockDecree.getKey(), lockDecree);

            // everything is fine :)
            return true;
        } else
        if (localLock.requester.equals(lockDecree.requester))
            // requester already owns the lock
            return true;
        else
            // lock does not belong to requester
            return false;

    }

    /**
     * Returns <code>true</code> if the requested lock can be granted by the
     * current node.
     * 
     * @param decree instance of <code>LockDecree</code> containing information
     * about the lock.
     */
    private boolean canLock(LockDecree decree) {
        // clean expired locks
        removeExpired(decree);

        LockDecree lock = (LockDecree)heldLocks.get(decree.getKey());
        if (lock == null)
            return true;
        else
            return lock.requester.equals(decree.requester);
    }

    /**
     * Returns <code>true</code> if the requested lock can be released by the
     * current node.
     * 
     * @param decree instance of {@link LockDecree} containing information
     * about the lock.
     */
    private boolean canRelease(LockDecree decree) {
        // clean expired locks
        removeExpired(decree);

        // we need to check only hold locks, because
        // prepared locks cannot contain the lock
        LockDecree lock = (LockDecree)heldLocks.get(decree.getKey());
        if (lock == null)
            // check if this holds...
            return true;
        else
            return lock.requester.equals(decree.requester);
    }

    /**
     * Removes expired locks.
     * 
     * @param decree instance of {@link LockDecree} describing the lock.
     */
    private void removeExpired(LockDecree decree) {
        // remove the invalid (expired) lock
        LockDecree localLock = (LockDecree)heldLocks.get(decree.getKey());
        if (localLock != null && !localLock.isValid())
            heldLocks.remove(localLock.getKey());
    }

    /**
     * Releases lock locally.
     * 
     * @param lockDecree instance of {@link LockDecree} describing the lock.
     */
    private boolean localRelease(LockDecree lockDecree) {
        // remove expired locks
        removeExpired(lockDecree);

        LockDecree localLock=
                (LockDecree) heldLocks.get(lockDecree.getKey());

        if(localLock == null) {
            // no lock exist
            return true;
        }
        else if(localLock.requester.equals(lockDecree.requester)) {
            // requester owns the lock, release the lock
            heldLocks.remove(lockDecree.getKey());
            return true;
        }
        else
        // lock does not belong to requester
            return false;
    }

    /**
     * Locks an object with <code>lockId</code> on behalf of the specified
     * <code>owner</code>.
     * 
     * @param lockId <code>Object</code> representing the object to be locked.
     * @param owner object that requests the lock.
     * @param timeout time during which group members should decide
     * whether to grant a lock or not.
     *
     * @throws LockNotGrantedException when the lock cannot be granted.
     * 
     * @throws ClassCastException if lockId or owner are not serializable.
     * 
     * @throws ChannelException if something bad happened to underlying channel.
     */
    public void lock(Object lockId, Object owner, int timeout)
        throws LockNotGrantedException, ChannelException
    {
        if (!(lockId instanceof Serializable) || !(owner instanceof Serializable))
            throw new ClassCastException("DistributedLockManager " +
                "works only with serializable objects.");

        boolean acquired = votingAdapter.vote(
            new AcquireLockDecree(lockId, owner, id), timeout);

        if (!acquired)
            throw new LockNotGrantedException("Lock cannot be granted.");
    }

    /**
     * Unlocks an object with <code>lockId</code> on behalf of the specified
     * <code>owner</code>.
     * 
     * since 2.2.9 this method is only a wrapper for 
     * unlock(Object lockId, Object owner, boolean releaseMultiLocked).
     * Use that with releaseMultiLocked set to true if you want to be able to
     * release multiple locked locks (for example after a merge)
     * 
     * @param lockId <code>long</code> representing the object to be unlocked.
     * @param owner object that releases the lock.
     *
     * @throws LockNotReleasedException when the lock cannot be released.
     * @throws ClassCastException if lockId or owner are not serializable.
     * 
     */
    public void unlock(Object lockId, Object owner)
        throws LockNotReleasedException, ChannelException
    {
        try {
            unlock(lockId, owner, false);
        } catch (LockMultiLockedException e) {
            // This should never happen when releaseMultiLocked is false
            log.error(ExternalStrings.DistributedLockManager_CAUGHT_MULTILOCKEDEXCEPTION_BUT_RELEASEMULTILOCKED_IS_FALSE, e);
        }
    }

    /**
     * Unlocks an object with <code>lockId</code> on behalf of the specified
     * <code>owner</code>.
     * @param lockId <code>long</code> representing the object to be unlocked.
     * @param owner object that releases the lock.
     * @param releaseMultiLocked releases also multiple locked locks. (eg. locks that are locked by another DLM after a merge)
     *
     * @throws LockNotReleasedException when the lock cannot be released.
     * @throws ClassCastException if lockId or owner are not serializable.
     * @throws LockMultiLockedException if releaseMultiLocked is true and a multiple locked lock has been released.
     */
    public void unlock(Object lockId, Object owner, boolean releaseMultiLocked)
        throws LockNotReleasedException, ChannelException, LockMultiLockedException
    {

        if (!(lockId instanceof Serializable) || !(owner instanceof Serializable))
            throw new ClassCastException("DistributedLockManager " +
                "works only with serializable objects.");

        ReleaseLockDecree releaseLockDecree = new ReleaseLockDecree(lockId, owner, id);
        boolean released = false;
        if (releaseMultiLocked) {
            released = votingAdapter.vote(releaseLockDecree, VOTE_TIMEOUT, this);
            if (releaseLockDecree.isMultipleLocked()) {
                throw new LockMultiLockedException("Lock was also locked by other DistributedLockManager(s)");
            }
        } else {
            released = votingAdapter.vote(releaseLockDecree, VOTE_TIMEOUT);
        }
        
        if (!released)
            throw new LockNotReleasedException("Lock cannot be unlocked.");
    }

    /**
     * Checks the list of prepared locks/unlocks to determine if we are in the
     * middle of the two-phase commit process for the lock acqusition/release.
     * Here we do not tolerate if the request comes from the same node on behalf
     * of the same owner.
     * 
     * @param preparedContainer either <code>preparedLocks</code> or
     * <code>preparedReleases</code> depending on the situation.
     * 
     * @param requestedDecree instance of <code>LockDecree</code> representing
     * the lock.
     */
    private boolean checkPrepared(HashMap preparedContainer, 
        LockDecree requestedDecree)
    {
        LockDecree preparedDecree =
            (LockDecree)preparedContainer.get(requestedDecree.getKey());

        // if prepared lock is not valid, remove it from the list
        if ((preparedDecree != null) && !preparedDecree.isValid()) {
            preparedContainer.remove(preparedDecree.getKey());

            preparedDecree = null;
        }

        if (preparedDecree != null) {
            if (requestedDecree.requester.equals(preparedDecree.requester))
                return true;
            else
                return false;
        } else
            // it was not prepared... sorry...
            return true;
    }

    /**
     * Prepare phase for the lock acquisition or release.
     * 
     * @param decree should be an instance <code>LockDecree</code>, if not,
     * we throw <code>VoteException</code> to be ignored by the
     * <code>VoteChannel</code>.
     * 
     * @return <code>true</code> when preparing the lock operation succeeds.
     * 
     * @throws VoteException if we should be ignored during voting.
     */
    public synchronized boolean prepare(Object decree) throws VoteException {
        if (!(decree instanceof LockDecree))
            throw new VoteException("Uknown decree type. Ignore me.");
            
        if (decree instanceof AcquireLockDecree) {
            AcquireLockDecree acquireDecree = (AcquireLockDecree)decree;
            if(log.isDebugEnabled()) log.debug("Preparing to acquire decree " + acquireDecree.lockId);
            
            if (!checkPrepared(preparedLocks, acquireDecree))
                // there is a prepared lock owned by third party
                return false;

            if (canLock(acquireDecree)) {
                preparedLocks.put(acquireDecree.getKey(), acquireDecree);
                return true;
            } else
                // we are unable to aquire local lock
                return false;
        } else
        if (decree instanceof ReleaseLockDecree) {
            ReleaseLockDecree releaseDecree = (ReleaseLockDecree)decree;
            

                if(log.isDebugEnabled()) log.debug("Preparing to release decree " + releaseDecree.lockId);

            if (!checkPrepared(preparedReleases, releaseDecree))
                // there is a prepared release owned by third party
                return false;

            if (canRelease(releaseDecree)) {
                preparedReleases.put(releaseDecree.getKey(), releaseDecree);
                // we have local lock and the prepared lock
                return true;
            } else
                // we were unable to aquire local lock
                return false;
        } else
        if (decree instanceof MultiLockDecree) {
            // Here we abuse the voting mechanism for notifying the other lockManagers of multiple locked objects.
            MultiLockDecree multiLockDecree = (MultiLockDecree)decree;
            
            if(log.isDebugEnabled()) {
                log.debug("Marking " + multiLockDecree.getKey() + " as multilocked");
            }

            LockDecree lockDecree = (LockDecree)heldLocks.get(multiLockDecree.getKey());
            if (lockDecree != null) {
                lockDecree.setMultipleLocked(true);
            }
            return true;
        }

        // we should not be here
        return false;
    }

    /**
     * Commit phase for the lock acquisition or release.
     * 
     * @param decree should be an instance <code>LockDecree</code>, if not,
     * we throw <code>VoteException</code> to be ignored by the
     * <code>VoteChannel</code>.
     * 
     * @return <code>true</code> when commiting the lock operation succeeds.
     * 
     * @throws VoteException if we should be ignored during voting.
     */
    public synchronized boolean commit(Object decree) throws VoteException {
        if (!(decree instanceof LockDecree))
            throw new VoteException("Uknown decree type. Ignore me.");

        if (decree instanceof AcquireLockDecree) {
            

                if(log.isDebugEnabled()) log.debug("Committing decree acquisition " + ((LockDecree)decree).lockId);
            
            if (!checkPrepared(preparedLocks, (LockDecree)decree))
                // there is a prepared lock owned by third party
                return false;

            if (localLock((LockDecree)decree)) {
                preparedLocks.remove(((LockDecree)decree).getKey());
                return true;
            } else
                return false;
        } else
        if (decree instanceof ReleaseLockDecree) {
            

                if(log.isDebugEnabled()) log.debug("Committing decree release " + ((LockDecree)decree).lockId);
            
            if (!checkPrepared(preparedReleases, (LockDecree)decree))
                // there is a prepared release owned by third party
                return false;

            if (localRelease((LockDecree)decree)) {
                preparedReleases.remove(((LockDecree)decree).getKey());
                return true;
            } else
                return false;
        } else
        if (decree instanceof MultiLockDecree) {
            return true;
        }

        // we should not be here
        return false;
    }

    /**
     * Abort phase for the lock acquisition or release.
     * 
     * @param decree should be an instance <code>LockDecree</code>, if not,
     * we throw <code>VoteException</code> to be ignored by the
     * <code>VoteChannel</code>.
     * 
     * @throws VoteException if we should be ignored during voting.
     */
    public synchronized void abort(Object decree) throws VoteException {
        if (!(decree instanceof LockDecree))
            throw new VoteException("Uknown decree type. Ignore me.");

        if (decree instanceof AcquireLockDecree) {
            

                if(log.isDebugEnabled()) log.debug("Aborting decree acquisition " + ((LockDecree)decree).lockId);
            
            if (!checkPrepared(preparedLocks, (LockDecree)decree))
                // there is a prepared lock owned by third party
                return;

            preparedLocks.remove(((LockDecree)decree).getKey());
        } else
        if (decree instanceof ReleaseLockDecree) {
            

                if(log.isDebugEnabled()) log.debug("Aborting decree release " + ((LockDecree)decree).lockId);
            
            if (!checkPrepared(preparedReleases, (LockDecree)decree))
                // there is a prepared release owned by third party
                return;

            preparedReleases.remove(((LockDecree)decree).getKey());
        }

    }

    
    /**
     * Processes the response list and votes like the default processResponses method with the consensusType VOTE_ALL
     * If the result of the voting is false, but this DistributedLockManager owns the lock, the result is changed to
     * true and the lock is released, but marked as multiple locked. (only in the prepare state to reduce traffic)
     * <p>
     * Note: we do not support voting in case of Byzantine failures, i.e.
     * when the node responds with the fault message.
     */
    public boolean processResponses(RspList responses, int consensusType, Object decree) throws ChannelException {
        if (responses == null) {
            return false;
        }

        int totalPositiveVotes = 0;
        int totalNegativeVotes = 0;

        for (int i = 0; i < responses.size(); i++) {
            Rsp response = (Rsp) responses.elementAt(i);

            switch (checkResponse(response)) {
                case PROCESS_SKIP:
                    continue;
                case PROCESS_BREAK:
                    return false;
            }

            VoteResult result = (VoteResult) response.getValue();

            totalPositiveVotes += result.getPositiveVotes();
            totalNegativeVotes += result.getNegativeVotes();
        }

        boolean voteResult = (totalNegativeVotes == 0 && totalPositiveVotes > 0);

        if (decree instanceof TwoPhaseVotingAdapter.TwoPhaseWrapper) {
            TwoPhaseVotingAdapter.TwoPhaseWrapper wrappedDecree = (TwoPhaseVotingAdapter.TwoPhaseWrapper)decree;
            if (wrappedDecree.isPrepare()) {
	            Object unwrappedDecree = wrappedDecree.getDecree();
	            if (unwrappedDecree instanceof ReleaseLockDecree) {
	                ReleaseLockDecree releaseLockDecree = (ReleaseLockDecree)unwrappedDecree;
	                LockDecree lock = null;
	                if ((lock = (LockDecree)heldLocks.get(releaseLockDecree.getKey())) != null) {
	                    // If there is a local lock...
	                    if (!voteResult) {
	                        // ... and another DLM voted negatively, but this DLM owns the lock
	                        // we inform the other node, that it's lock is multiple locked
	                        if (informLockingNodes(releaseLockDecree)) {
	                        
		                        // we set the local lock to multiple locked
		                        lock.setMultipleLocked(true);
		                        
		                        voteResult = true;
	                        }
	                    }
	                    if (lock.isMultipleLocked()) {
	                        //... and the local lock is marked as multilocked
	                        // we mark the releaseLockDecree als multiple locked for evaluation when unlock returns
	                        releaseLockDecree.setMultipleLocked(true);
	                    }
	                }
	            }
            }
        }

        return voteResult;
    }

    /**
     * This method checks the response and says the processResponses() method
     * what to do.
     * @return PROCESS_CONTINUE to continue calculating votes,
     * PROCESS_BREAK to stop calculating votes from the nodes,
     * PROCESS_SKIP to skip current response.
     * @throws ChannelException when the response is fatal to the
     * current voting process.
     */
    private int checkResponse(Rsp response) throws ChannelException {

        if (!response.wasReceived()) {

            if (log.isDebugEnabled())
                log.debug("Response from node " + response.getSender() + " was not received.");

            throw new ChannelException("Node " + response.getSender() + " failed to respond.");
        }

        if (response.wasSuspected()) {

            if (log.isDebugEnabled())
                log.debug("Node " + response.getSender() + " was suspected.");

            return PROCESS_SKIP;
        }

        Object object = response.getValue();

        // we received exception/error, something went wrong
        // on one of the nodes... and we do not handle such faults
        if (object instanceof Throwable) {
            throw new ChannelException("Node " + response.getSender() + " is faulty.");
        }

        if (object == null) {
            return PROCESS_SKIP;
        }

        // it is always interesting to know the class that caused failure...
        if (!(object instanceof VoteResult)) {
            String faultClass = object.getClass().getName();

            // ...but we do not handle byzantine faults
            throw new ChannelException("Node " + response.getSender() + " generated fault (class " + faultClass + ')');
        }

        // what if we received the response from faulty node?
        if (object instanceof FailureVoteResult) {

            if (log.isErrorEnabled())
                log.error(ExternalStrings.DistributedLockManager_0, ((FailureVoteResult) object).getReason());

            return PROCESS_BREAK;
        }

        // everything is fine :)
        return PROCESS_CONTINUE;
    }
    
    private boolean informLockingNodes(ReleaseLockDecree releaseLockDecree) throws ChannelException {
        return votingAdapter.vote(new MultiLockDecree(releaseLockDecree), VOTE_TIMEOUT);
    }
    
    /**
     * This class represents the lock
     */
    public static class LockDecree implements Serializable {
        private static final long serialVersionUID = -7422058144576778340L;

        protected final Object lockId;
        protected final Object requester;
        protected final Object managerId;

        protected boolean commited;
        
        private boolean multipleLocked = false;

        protected LockDecree(Object lockId, Object requester, Object managerId) {
            this.lockId = lockId;
            this.requester = requester;
            this.managerId = managerId;
        }

        /**
         * Returns the key that should be used for Map lookup.
         */
        public Object getKey() { return lockId; }

        /**
         * This is a place-holder for future lock expiration code.
         */
        public boolean isValid() { return true; }

        public void commit() { this.commited = true; }

        /**
         * @return Returns the multipleLocked.
         */
        public boolean isMultipleLocked() {
            return multipleLocked;
        }
        /**
         * @param multipleLocked The multipleLocked to set.
         */
        public void setMultipleLocked(boolean multipleLocked) {
            this.multipleLocked = multipleLocked;
        }
        /**
         * This is hashcode from the java.lang.Long class.
         */
        @Override // GemStoneAddition
        public int hashCode() {
            return lockId.hashCode();
        }

        @Override // GemStoneAddition
        public boolean equals(Object other) {

            if (other instanceof LockDecree) {
                return ((LockDecree)other).lockId.equals(this.lockId);
            } else {
                return false;
            }
        }
    }


    /**
     * This class represents the lock to be released.
     */
    public static class AcquireLockDecree extends LockDecree  {
        private static final long serialVersionUID = 7608853900623293300L;
        private final long creationTime;

        protected AcquireLockDecree(Object lockId, Object requester, Object managerId) {
            super(lockId, requester, managerId);
            this.creationTime = System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object o) { // GemStoneAddition for findbugs
          return super.equals(o);
        }
        
        @Override
        public int hashCode() { // GemStoneAddition for findbugs
          return super.hashCode();
        }
        
        /**
         * Lock aquire decree is valid for a <code>ACQUIRE_EXPIRATION</code>
         * time after creation and if the lock is still valid (in the
         * future locks will be leased for a predefined period of time).
         */
        @Override // GemStoneAddition
        public boolean isValid() {
            boolean result =  super.isValid();

            if (!commited && result)
                result = ((creationTime + ACQUIRE_EXPIRATION) > System.currentTimeMillis());

            return result;
        }

    }

    /**
     * This class represents the lock to be released.
     */
    public static class ReleaseLockDecree extends LockDecree  {
        private static final long serialVersionUID = -159320406385342426L;
        ReleaseLockDecree(Object lockId, Object requester, Object managerId) {
            super(lockId, requester, managerId);
        }
    }
    
    /**
     * This class represents the lock that has to be marked as multilocked 
     */
    public static class MultiLockDecree extends LockDecree  {
        private static final long serialVersionUID = -8775726661815938941L;
        MultiLockDecree(Object lockId, Object requester, Object managerId) {
            super(lockId, requester, managerId);
        }

        MultiLockDecree(ReleaseLockDecree releaseLockDecree) {
            super(releaseLockDecree.lockId, releaseLockDecree.requester, releaseLockDecree.managerId);
        }
    }
}
