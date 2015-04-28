/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RequestHandler.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.Message;


public interface RequestHandler {
    Object handle(Message msg);
}
