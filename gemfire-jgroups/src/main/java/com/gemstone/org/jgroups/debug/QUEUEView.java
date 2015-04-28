/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: QUEUEView.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package com.gemstone.org.jgroups.debug;

import javax.swing.*;

/**
   Shows the internals of the QUEUE protocol. Imlements QUEUE.Observer, gets called before an event
   is added to the (internal) up- or down-queue of QUEUE (<em>not</em>Protocol !). This allows
   to see what's going on in QUEUE. QUEUEView can also query the attributes of QUEUE, e.g. up_vec,
   dn_vec etc.<br>
   QUEUEView is implemented as a JPanel, and not as a JFrame, so it can be displayed inside the
   Debugger's own view.
   @author  Bela Ban, July 23 2000
*/


public class QUEUEView extends ProtocolSpecificView {
    private static final long serialVersionUID = 1962110363758525507L;

    public QUEUEView() {  // must have a public constructor, will be created by means of reflection
	add(new JButton("QUEUEView: hello world"));
    }
}
