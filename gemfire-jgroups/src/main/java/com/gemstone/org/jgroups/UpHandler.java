/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: UpHandler.java,v 1.2 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Provides a way of taking over a channel's tasks. 
 */
public interface UpHandler {
	/**
	 * Invoked for all channel events except connection management and state transfer.
	 * @param evt
	 */
    void up(Event evt);
}
