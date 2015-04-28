/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.util.RspList;


/**
 * VoteResultProcessor
 * Applications that use the VotingAdapter and/or TwoPhaseVotingAdapter can pass an implementation of this down the vote
 * calls, to intercept processing of the VoteResults returned by other nodes.
 * See the source of {@link com.gemstone.org.jgroups.blocks.DistributedLockManager} for an example implementation.  
 * 
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: VoteResponseProcessor.java,v 1.2 2005/07/17 11:36:40 chrislott Exp $
 */
public interface VoteResponseProcessor {
    /**
     * Processes the responses returned by the other nodes.
     * @param responses The responses
     * @param consensusType The consensusType of the vote
     * @param decree The vote decree
     * @return boolean
     * @throws ChannelException
     */
    public boolean processResponses(RspList responses, int consensusType, Object decree) throws ChannelException;
}
