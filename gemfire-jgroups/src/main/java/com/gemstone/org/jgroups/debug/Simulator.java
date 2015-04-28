/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.debug;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolStack;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Tests one or more protocols independently. Look at com.gemstone.org.jgroups.tests.FCTest for an example of how to use it.
 * @author Bela Ban
 * @version $Id: Simulator.java,v 1.6 2005/08/22 14:12:53 belaban Exp $
 */
public class Simulator {
    private Protocol[] protStack=null;
    private ProtocolAdapter ad=new ProtocolAdapter();
    ProtocolStack prot_stack=null;
    protected/*GemStoneAddition*/ Receiver r=null;
    protected/*GemStoneAddition*/ Protocol top=null, bottom=null;
    protected/*GemStoneAddition*/ Queue send_queue=new Queue();
    protected/*GemStoneAddition*/ Thread send_thread; // GemStoneAddition - accesses synchronized on this
    protected/*GemStoneAddition*/ Queue recv_queue=new Queue();
    protected/*GemStoneAddition*/ Thread recv_thread; // GemStoneAddition - accesses synchronized on this


    /** HashMap from Address to Simulator. */
    protected/*GemStoneAddition*/ final HashMap addrTable=new HashMap();
    protected/*GemStoneAddition*/ Address local_addr=null;
    private View view;

    public interface Receiver {
        void receive(Event evt);
    }


    public void setProtocolStack(Protocol[] stack) {
        this.protStack=stack;
        this.protStack[0].setUpProtocol(ad);
        this.protStack[this.protStack.length-1].setDownProtocol(ad);
        top=protStack[0];
        bottom=this.protStack[this.protStack.length-1];

        prot_stack=new ProtocolStack();

        if(protStack.length > 1) {
            for(int i=0; i < protStack.length; i++) {
                Protocol p1=protStack[i];
                p1.setProtocolStack(prot_stack);
                Protocol p2=i+1 >= protStack.length? null : protStack[i+1];
                if(p2 != null) {
                    p1.setDownProtocol(p2);
                    p2.setUpProtocol(p1);
                }
            }
        }
    }

    public String dumpStats() {
        StringBuffer sb=new StringBuffer();
        for(int i=0; i < protStack.length; i++) {
            Protocol p1=protStack[i];
            sb.append(p1.getName()).append(":\n").append(p1.dumpStats()).append("\n");
        }
        return sb.toString();
    }

    public void addMember(Address addr) {
        addMember(addr, this);
    }

    public void addMember(Address addr, Simulator s) {
        addrTable.put(addr, s);
    }

    public void setLocalAddress(Address addr) {
        this.local_addr=addr;
    }

    public void setView(View v) {
        this.view=v;
    }

    public void setReceiver(Receiver r) {
        this.r=r;
    }

    public void send(Event evt) {
        top.down(evt);
    }

    public void receive(Event evt) {
        try {
            Event copy;
            if(evt.getType() == Event.MSG && evt.getArg() != null) {
                copy=new Event(Event.MSG, ((Message)evt.getArg()).copy());
            }
            else
                copy=evt;

            recv_queue.add(copy);
        }
        catch(QueueClosedException e) {
        }
    }

    synchronized /* GemStoneAddition */ public void start() throws Exception {
       if(local_addr == null)
            throw new Exception("local_addr has to be non-null");
        if(protStack == null)
            throw new Exception("protocol stack is null");

        bottom.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
        if(view != null) {
            Event view_evt=new Event(Event.VIEW_CHANGE, view);
            bottom.up(view_evt);
            top.down(view_evt);
        }

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.setProtocolStack(prot_stack);
        }

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.init();
        }

        for(int i=0; i < protStack.length; i++) {
            Protocol p=protStack[i];
            p.start();
        }


        send_thread=new Thread() {
          @Override // GemStoneAddition  
            public void run() {
                Event evt;
                for (;;) { // GemStoneAddition remove coding anti-pattern
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneADdition
                    try {
                        evt=(Event)send_queue.remove();
                        if(evt.getType() == Event.MSG) {
                            Message msg=(Message)evt.getArg();
                            Address dst=msg.getDest();
                            if(msg.getSrc() == null)
                                ((Message)evt.getArg()).setSrc(local_addr);
                            Simulator s;
                            if(dst == null) {
                                for(Iterator it=addrTable.values().iterator(); it.hasNext();) {
                                    s=(Simulator)it.next();
                                    s.receive(evt);
                                }
                            }
                            else {
                                s=(Simulator)addrTable.get(dst);
                                if(s != null)
                                    s.receive(evt);
                            }
                        }
                    }
                    catch (InterruptedException ie) { // GemStoneAddition
                        // no need to set interrupt bit, we're exiting the thread.
                        break;
                    }
                    catch(QueueClosedException e) {
//                        send_thread=null; GemStoneAddition
                        break;
                    }
                }
            }
        };
        send_thread.start();


        recv_thread=new Thread() {
          @Override // GemStoneAddition  
            public void run() {
                Event evt;
                for (;;) { // GemStoneAddition remove coding anti-pattern
                  if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                    try {
                        evt=(Event)recv_queue.remove();
                        bottom.up(evt);
                    }
                    catch (InterruptedException ie) { // GemStoneAddition
                        // no need to set interrupt bit, we're exiting
                        break;
                    }
                    catch(QueueClosedException e) {
//                        recv_thread=null;
                        break;
                    }
                }
            }
        };
        recv_thread.start();
    }

    synchronized /* GemStoneAddition */ public void stop() {
        if (recv_thread != null) recv_thread.interrupt(); // GemStoneAddition
        recv_thread=null;
        recv_queue.close(false);
        if (send_thread != null) send_thread.interrupt(); // GemStoneAddition
        send_thread=null;
        send_queue.close(false);
    }




    class ProtocolAdapter extends Protocol {

      @Override // GemStoneAddition  
        public String getName() {
            return "ProtocolAdapter";
        }

      @Override // GemStoneAddition  
        public void up(Event evt) {
            if(r != null)
                r.receive(evt);
        }

        /** send to unicast or multicast destination */
      @Override // GemStoneAddition  
        public void down(Event evt) {
            try {
                send_queue.add(evt);
            }
            catch(QueueClosedException e) {
            }
        }
    }



}
