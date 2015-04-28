/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RspCollector.java,v 1.2 2004/03/30 06:47:12 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;


public interface RspCollector {
    void receiveResponse(Message msg);
    void suspect(Address mbr);
    void viewChange(View new_view);
}
