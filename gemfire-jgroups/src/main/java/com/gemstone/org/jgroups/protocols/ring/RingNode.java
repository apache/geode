/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
//$Id: RingNode.java,v 1.2 2004/03/30 06:47:20 belaban Exp $

package com.gemstone.org.jgroups.protocols.ring;

import com.gemstone.org.jgroups.stack.IpAddress;

import java.util.Vector;

public interface RingNode
{
   Object receiveToken(int timeout) throws TokenLostException;

   Object receiveToken() throws TokenLostException;

   void passToken(Object token) throws TokenLostException;

   IpAddress getTokenReceiverAddress();

   void reconfigure(Vector newMembers);

   void tokenArrived(Object token);
}
