/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ChannelNotConnectedException.java,v 1.2 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Thrown if an operation is attemped on an unconnected channel.
 */
public class ChannelNotConnectedException extends ChannelException {
private static final long serialVersionUID = -1556386890787640150L;

    public ChannelNotConnectedException() {
    }

    public ChannelNotConnectedException(String reason) {
        super(reason);
    }

    @Override // GemStoneAddition
    public String toString() {
        return "ChannelNotConnectedException";
    }
}
