/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups;

/**
 * Globals used by JGroups packages.
 * 
 * @author Bela Ban Mar 29, 2004
 * @version $Id: Global.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $
 */
public class Global {
    /** Allows for conditional compilation; e.g., if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info(...) would be removed from the code
	(if recompiled) when this flag is set to false. Therefore, code that should be removed from the final
	product should use if(log.isTraceEnabled()) rather than .
    */
    public static final boolean debug=false;

    /**
     * Used to determine whether to copy messages (copy=true) in retransmission tables,
     * or whether to use references (copy=false). Once copy=false has worked for some time, this flag
     * will be removed entirely
     */
    public static final boolean copy=false;
    

    /**
     * Value is {@value}.
     */
    public static final int BYTE_SIZE  = 1;
    /**
     * Value is {@value}.
     */
    public static final int SHORT_SIZE = 2;
    /**
    * Value is {@value}.
     */
    public static final int INT_SIZE   = 4;
    /**
     * Value is {@value}.
     */
    public static final int LONG_SIZE  = 8;
}
