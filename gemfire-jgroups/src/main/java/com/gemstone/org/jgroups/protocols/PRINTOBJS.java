/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PRINTOBJS.java,v 1.4 2005/05/30 14:31:07 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;


public class PRINTOBJS extends Protocol {

    public PRINTOBJS() {
    }

    @Override // GemStoneAddition  
    public String getName() {
        return "PRINTOBJS";
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Object obj=null;
//        byte[] buf; GemStoneAddition
        Message msg;

        if(evt.getType() != Event.MSG) {
            System.out.println("------------ PRINTOBJS (received event) ----------------");
            System.out.println(evt);
            System.out.println("--------------------------------------------------------");
            passUp(evt);
            return;
        }

        msg=(Message)evt.getArg();
        if(msg.getLength() > 0) {
            try {
                obj=msg.getObject();
            }
            catch(ClassCastException cast_ex) {
                System.out.println("------------ PRINTOBJS (received) ----------------------");
                System.out.println(msg);
                System.out.println("--------------------------------------------------------");
                passUp(evt);
                return;
            }
            catch(Exception e) {
                log.error(e);
            }

            System.out.println("------------ PRINTOBJS (received) ----------------------");
            System.out.println(obj);
            System.out.println("--------------------------------------------------------");
        }
        else
            System.out.println("------- PRINTOBJS (received null msg from " + msg.getSrc() + ", headers are " +
                    msg.printObjectHeaders() + ") --------");

        passUp(evt);
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Object obj=null;
//        byte[] buf; GemStoneAddition
        Message msg;

        if(evt.getType() != Event.MSG) {
            System.out.println("------------ PRINTOBJS (sent event) --------------------");
            System.out.println(evt);
            System.out.println("--------------------------------------------------------");
            passDown(evt);
            return;
        }

        msg=(Message)evt.getArg();
        if(msg.getLength() > 0) {
            try {
                obj=msg.getObject();
            }
            catch(ClassCastException cast_ex) {
                System.out.println("------------ PRINTOBJS (sent) --------------------------");
                System.out.println(msg);
                System.out.println("--------------------------------------------------------");
                passDown(evt);
                return;
            }
            catch(Exception e) {
                log.error(e);
            }

            System.out.println("------------ PRINTOBJS (sent) --------------------------");
            System.out.println(obj);
            System.out.println("--------------------------------------------------------");
        }
        else
            System.out.println("------- PRINTOBJS (sent null msg to " + msg.getDest() + ", headers are " +
                    msg.printObjectHeaders() + " ) -------------");

        passDown(evt);
    }


    public void reset() {
        System.out.println("PRINTOBJS protocol is reset");
    }

    @Override // GemStoneAddition  
    public String toString() {
        return "Protocol PRINTOBJS";
    }


}
