/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.ChannelException;

/**
 * This exception is thrown when voting listener cannot vote on the
 * specified decree.
 *  
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class VoteException extends ChannelException  {
private static final long serialVersionUID = -741925330540432706L;

    public VoteException(String msg) { super(msg); }
}
