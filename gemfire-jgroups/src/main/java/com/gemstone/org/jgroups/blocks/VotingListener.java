/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

/**
 * Implemetations of this interface are able to participate in voting process.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public interface VotingListener {
    /**
     * Each member is able to vote with <code>true</code> or <code>false</code>
     * messages. If the member does not know what to do with the
     * <code>decree</code> it should throw <code>VoteException</code>. Doing
     * this he will be excluded from voting process and will not influence
     * the result.
     * 
     * @param decree object representing the decree of current voting.
     * 
     * @throws VoteException if listener does not know the meaning of the 
     * decree and wants to be excluded from this voting.
     */
    boolean vote(Object decree) throws VoteException;
}
