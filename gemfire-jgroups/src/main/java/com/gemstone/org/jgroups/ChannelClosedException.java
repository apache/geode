/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ChannelClosedException.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Thrown if an operation is attemped on a closed channel.
 */
public class ChannelClosedException extends ChannelException {
private static final long serialVersionUID = 3183749334840801913L;

    public ChannelClosedException() {
        super();
    }

    public ChannelClosedException(String msg) {
        super(msg);
    }

    @Override // GemStoneAddition
    public String toString() {
        return "ChannelClosedException";
    }
}
