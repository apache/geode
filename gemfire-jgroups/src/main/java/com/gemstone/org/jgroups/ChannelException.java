/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ChannelException.java,v 1.6 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * This class represents the super class for all exception types thrown by
 * JGroups.
 */
public class ChannelException extends Exception {
private static final long serialVersionUID = -6809241131485085619L;

    public ChannelException() {
        super();
    }

    public ChannelException(String reason) {
        super(reason);
    }

    public ChannelException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
