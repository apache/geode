/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ChannelFactory.java,v 1.2 2004/07/31 22:14:18 jiwils Exp $

package com.gemstone.org.jgroups;

/**
   A channel factory takes care of creation of channel implementations. Subclasses will create
   different implementations.
 */
public interface ChannelFactory {

    /**
       Creates an instance implementing the <code>Channel</code> interface.
       @param properties The specification of the protocol stack (underneath the channel).
              A <code>null</code> value means use the default properties.
       @exception ChannelException Thrown when the creation of the channel failed, e.g.
                  the <code>properties</code> specified were incompatible (e.g. a missing
		  UDP layer etc.)
       @deprecated Channel factories should pass configuration information
                   related to the protocol stack during construction or via
                   another method before attempting to create any channels.
     */
    @Deprecated // GemStoneAddition
    Channel createChannel(Object properties) throws ChannelException;

    /**
     * Creates an instance implementing the <code>Channel</code> interface.
     * <p>
     * Protocol stack configuration information should be passed to implementing
     * factories before this method is called.
     *
     * @throws ChannelException if the creation of the channel failed.
     */
     Channel createChannel() throws ChannelException;
}
