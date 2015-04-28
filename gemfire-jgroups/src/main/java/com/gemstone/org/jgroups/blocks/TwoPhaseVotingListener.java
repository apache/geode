/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

/**
 * Implementations of this interface can participate in two-phase voting process.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public interface TwoPhaseVotingListener {
    /**
     * This is voting if the decree is acceptable to the party.
     * @return <code>true</code> if the decree is acceptable.
     * @throws VoteException if the decree type is unknown or listener
     * does not want to vote on it.
     */
    boolean prepare(Object decree) throws VoteException;

    /**
     * This is voting on the commiting the decree.
     * @return <code>true</code> is the decree is commited.
     * @throws VoteException if the decree type is unknown or listener
     * does not want to vote on it.
     */
    boolean commit(Object decree) throws VoteException;

    /**
     * This is unconditional abort of the previous voting on the decree.
     * @throws VoteException if the listener ignores the abort.
     */
    void abort(Object decree) throws VoteException;

}
