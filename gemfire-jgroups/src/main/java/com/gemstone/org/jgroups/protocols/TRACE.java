/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TRACE.java,v 1.2 2004/03/30 06:47:21 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.stack.Protocol;



public class TRACE extends Protocol {

    public TRACE() {}

    @Override // GemStoneAddition  
    public String        getName()             {return "TRACE";}

    

    @Override // GemStoneAddition  
    public void up(Event evt) {
	System.out.println("---------------- TRACE (received) ----------------------");
	System.out.println(evt);
	System.out.println("--------------------------------------------------------");
	passUp(evt);
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
	System.out.println("------------------- TRACE (sent) -----------------------");
	System.out.println(evt);
	System.out.println("--------------------------------------------------------");
	passDown(evt);
    }


    @Override // GemStoneAddition  
    public String toString() {
	return "Protocol TRACE";
    }


}
