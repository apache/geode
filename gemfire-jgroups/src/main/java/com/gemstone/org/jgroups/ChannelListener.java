/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ChannelListener.java,v 1.1.1.1 2003/09/09 01:24:07 belaban Exp $

package com.gemstone.org.jgroups;


/**
 * Allows a listener to be notified when important channel events occur. For example, when
 * a channel is closed, a PullPushAdapter can be notified, and stop accordingly.
 */
public interface ChannelListener {
    void channelConnected(Channel channel);
    void channelDisconnected(Channel channel);
    void channelClosed(Channel channel);
    void channelShunned();
    void channelReconnected(Address addr);
}
