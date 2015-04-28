/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: QueueClosedException.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package com.gemstone.org.jgroups.util;


public class QueueClosedException extends Exception {
private static final long serialVersionUID = -5118272971200348252L;

    public QueueClosedException() {

    }

    public QueueClosedException( String msg )
    {
        super( msg );
    }

    @Override // GemStoneAddition
    public String toString() {
        if ( this.getMessage() != null )
            return "QueueClosedException:" + this.getMessage();
        else
            return "QueueClosedException";
    }
}
