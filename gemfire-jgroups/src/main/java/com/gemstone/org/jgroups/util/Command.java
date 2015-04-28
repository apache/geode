/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Command.java,v 1.2 2005/07/17 11:33:58 chrislott Exp $

package com.gemstone.org.jgroups.util;

/**
  * The Command patttern (see Gamma et al.). Implementations would provide their
  * own <code>execute</code> method.
  * @author Bela Ban
  */
public interface Command {
    boolean execute();
}
