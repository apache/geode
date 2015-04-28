/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: SuspectedException.java,v 1.2 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 */
public class SuspectedException extends Exception {
private static final long serialVersionUID = -7362834003171175180L;
    Object suspect=null;

    public SuspectedException()                {}
    public SuspectedException(Object suspect)  {this.suspect=suspect;}

    @Override // GemStoneAddition
    public String toString() {return "SuspectedException";}
}
