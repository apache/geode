/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ProtocolSpecificView.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package com.gemstone.org.jgroups.debug;

import javax.swing.*;

/**
   Abstract class for all protocol-specific views, e.g. QUEUEView.
   @author  Bela Ban, July 23 2000
*/


abstract public class ProtocolSpecificView extends JPanel {
    
    public ProtocolSpecificView() {
	super();
    }

    
}
