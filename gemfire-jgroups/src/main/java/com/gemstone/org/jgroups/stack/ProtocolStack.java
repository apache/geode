/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ProtocolStack.java,v 1.24 2005/09/29 12:10:03 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.conf.ClassConfigurator;
import com.gemstone.org.jgroups.protocols.TP;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.TimeScheduler;

import java.util.*;




/**
 * A ProtocolStack manages a number of protocols layered above each other. It creates all
 * protocol classes, initializes them and, when ready, starts all of them, beginning with the
 * bottom most protocol. It also dispatches messages received from the stack to registered
 * objects (e.g. channel, GMP) and sends messages sent by those objects down the stack.<p>
 * The ProtocolStack makes use of the Configurator to setup and initialize stacks, and to
 * destroy them again when not needed anymore
 * @author Bela Ban
 */
public class ProtocolStack extends Protocol implements Transport {
    private Protocol                top_prot=null;
    private Protocol                bottom_prot=null;
    private final Configurator      conf=new Configurator();
    private String                  setup_string;
    private JChannel                channel=null;
    private boolean                 stopped=true;
    public final  TimeScheduler     timer=new TimeScheduler(60000);
    // final Promise                   ack_promise=new Promise();

    /** Used to sync on START/START_OK events for start()*/
    Promise                         start_promise=null;

    /** used to sync on STOP/STOP_OK events for stop() */
    Promise                         stop_promise=null;

    public static final int         ABOVE=1; // used by insertProtocol()
    public static final int         BELOW=2; // used by insertProtocol()
    
    public GFBasicAdapter gfBasicFunctions = new GFBasicAdapterImpl(); // GemStoneAddition
    public GFPeerAdapter gfPeerFunctions = new GFPeerAdapterImpl(); // GemStoneAddition

    public boolean enableClockStats; // GemStoneAddition
    public boolean enableJgStackStats; // GemStoneAddition
    private boolean hasTriedJoinShortcut; // GemStoneAddition

    /**
     * Make sure that the TimeScheduler class gets loaded
     * 
     * @see SystemFailure#loadEmergencyClasses()
     */
    static public void loadEmergencyClasses() {
      TimeScheduler.loadEmergencyClasses();
    }

    /**
     * Stops the timeScheduler
     * 
     * @see SystemFailure#emergencyClose()
     */
    public void emergencyClose() {
      timer.emergencyClose();
    }
    
    public ProtocolStack(JChannel channel, String setup_string) throws ChannelException {
        this.setup_string=setup_string;
        this.channel=channel;
        ClassConfigurator.getInstance(true); // will create the singleton
    }

    /** Only used by Simulator; don't use */
    public ProtocolStack() {

    }
    
    // GemStoneAddition - create and start a stack for testing
    public static ProtocolStack createTestStack(JChannel channel, Protocol top) {
      ProtocolStack instance = new ProtocolStack();
      instance.channel = channel;
      instance.top_prot = top;
      top.setProtocolStack(instance);
      Protocol bottom = top;
      while (bottom.getDownProtocol() != null) {
        bottom = bottom.getDownProtocol();
        bottom.setProtocolStack(instance);
      }
      instance.bottom_prot = bottom;
      instance.top_prot.setUpProtocol(instance);
      instance.conf.startProtocolStack(bottom);
      return instance;
    }


    public Channel getChannel() {
        return channel;
    }
    
    public Protocol getTopProtocol() { // GemStoneAddition
      return this.top_prot;
    }
    
    public Protocol getBottomProtocol() { // GemStoneAddition
      return this.bottom_prot;
    }

    /** Returns all protocols in a list, from top to bottom. <em>These are not copies of protocols,
     so modifications will affect the actual instances !</em> */
    public Vector getProtocols() {
        Protocol p;
        Vector   v=new Vector();

        p=top_prot;
        while(p != null) {
            v.addElement(p);
            p=p.getDownProtocol();
        }
        return v;
    }

    /**
     *
     * @return Map<String,Map<key,val>>
     */
    @Override // GemStoneAddition
    public Map dumpStats() {
        Protocol p;
        Map retval=new HashMap(), tmp;
        String prot_name;

        p=top_prot;
        while(p != null) {
            prot_name=p.getName();
            tmp=p.dumpStats();
            if(prot_name != null && tmp != null)
                retval.put(prot_name, tmp);
            p=p.getDownProtocol();
        }
        return retval;
    }

    public String dumpTimerQueue() {
        return timer.dumpTaskQueue();
    }

    /**
     * Prints the names of the protocols, from the bottom to top. If include_properties is true,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        StringBuffer sb=new StringBuffer();
        Protocol     prot=top_prot;
        Properties   tmpProps;
        String       name;
        Map.Entry    entry;

        while(prot != null) {
            name=prot.getName();
            if(name != null) {
                if("ProtocolStack".equals(name))
                    break;
                sb.append(name);
                if(include_properties) {
                    tmpProps=prot.getProperties();
                    if(tmpProps != null) {
                        sb.append('\n');
                        for(Iterator it=tmpProps.entrySet().iterator(); it.hasNext();) {
                            entry=(Map.Entry)it.next();
                            sb.append(entry + "\n");
                        }
                    }
                }
                sb.append('\n');

                prot=prot.getDownProtocol();
            }
        }

        return sb.toString();
    }

    public String printProtocolSpecAsXML() {
        StringBuffer sb=new StringBuffer();
        Protocol     prot=bottom_prot;
        Properties   tmpProps;
        String       name;
        Map.Entry    entry;
        int len, max_len=30;

        sb.append("<config>\n");
        while(prot != null) {
            name=prot.getName();
            if(name != null) {
                if("ProtocolStack".equals(name))
                    break;
                sb.append("  <").append(name).append(" ");
                tmpProps=prot.getProperties();
                if(tmpProps != null) {
                    len=name.length();
                    String s;
                    for(Iterator it=tmpProps.entrySet().iterator(); it.hasNext();) {
                        entry=(Map.Entry)it.next();
                        s=entry.getKey() + "=\"" + entry.getValue() + "\" ";
                        if(len + s.length() > max_len) {
                            sb.append("\n       ");
                            len=8;
                        }
                        sb.append(s);
                        len+=s.length();
                    }
                }
                sb.append("/>\n");
                prot=prot.getUpProtocol();
            }
        }
        sb.append("</config>");

        return sb.toString();
    }


    public void setup() throws Exception {
        if(top_prot == null) {
            top_prot=conf.setupProtocolStack(setup_string, this); // calls init() on each protocol
            if(top_prot == null)
                throw new Exception("ProtocolStack.setup(): couldn't create protocol stack");
            top_prot.setUpProtocol(this);
            bottom_prot=conf.getBottommostProtocol(top_prot);
            conf.startProtocolStack(bottom_prot);        // sets up queues and threads
        }
    }




    /**
     * Creates a new protocol given the protocol specification.
     * @param prot_spec The specification of the protocol. Same convention as for specifying a protocol stack.
     *                  An exception will be thrown if the class cannot be created. Example:
     *                  <pre>"VERIFY_SUSPECT(timeout=1500)"</pre> Note that no colons (:) have to be
     *                  specified
     * @return Protocol The newly created protocol
     * @exception Exception Will be thrown when the new protocol cannot be created
     */
    public Protocol createProtocol(String prot_spec) throws Exception {
        return conf.createProtocol(prot_spec, this);
    }






    /**
     * Inserts an already created (and initialized) protocol into the protocol list. Sets the links
     * to the protocols above and below correctly and adjusts the linked list of protocols accordingly.
     * Note that this method may change the value of top_prot or bottom_prot.
     * @param prot The protocol to be inserted. Before insertion, a sanity check will ensure that none
     *             of the existing protocols have the same name as the new protocol.
     * @param position Where to place the protocol with respect to the neighbor_prot (ABOVE, BELOW)
     * @param neighbor_prot The name of the neighbor protocol. An exception will be thrown if this name
     *                      is not found
     * @exception Exception Will be thrown when the new protocol cannot be created, or inserted.
     */
    public void insertProtocol(Protocol prot, int position, String neighbor_prot) throws Exception {
        conf.insertProtocol(prot, position, neighbor_prot, this);
    }





    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public void removeProtocol(String prot_name) throws Exception {
        conf.removeProtocol(prot_name);
    }


    /** Returns a given protocol or null if not found */
    public Protocol findProtocol(String name) {
        Protocol tmp=top_prot;
        String   prot_name;
        while(tmp != null) {
            prot_name=tmp.getName();
            if(prot_name != null && prot_name.equals(name))
                return tmp;
            tmp=tmp.getDownProtocol();
        }
        return null;
    }


    @Override // GemStoneAddition
    public void destroy() {
        if(top_prot != null) {
            conf.stopProtocolStack(top_prot);           // destroys msg queues and threads
            top_prot=null;
        }
    }



    /**
     * Start all layers. The {@link Protocol#start()} method is called in each protocol,
     * <em>from top to bottom</em>.
     * Each layer can perform some initialization, e.g. create a multicast socket
     */
    public void startStack() throws Exception {
        Object start_result=null;
        if(stopped == false) return;

        timer.start();

        if(start_promise == null)
            start_promise=new Promise();
        else
            start_promise.reset();

        down(new Event(Event.START));
        start_result=start_promise.getResult(0);
        if(start_result != null && start_result instanceof Throwable) {
            if(start_result instanceof Exception)
                throw (Exception)start_result;
            else
                throw new Exception("ProtocolStack.start(): exception is " + start_result);
        }

        stopped=false;
    }



    @Override // GemStoneAddition
    public void startUpHandler() {
        // DON'T REMOVE !!!!  Avoids a superfluous thread
    }

    @Override // GemStoneAddition
    public void startDownHandler() {
        // DON'T REMOVE !!!!  Avoids a superfluous thread
    }


    /**
     * Iterates through all the protocols <em>from top to bottom</em> and does the following:
     * <ol>
     * <li>Waits until all messages in the down queue have been flushed (ie., size is 0)
     * <li>Calls stop() on the protocol
     * </ol>
     */
    public void stopStack() {
        if(timer != null) {
            try {
                timer.stop();
            }
            catch(Exception ex) {
            }
        }

        if(stopped) return;

        if(stop_promise == null)
            stop_promise=new Promise();
        else
            stop_promise.reset();
        // GemStoneAddition - stop TP protocol first so that noise (suspect/xmit reqs/etc)
        // gets in the way of dismantling things.  See bug #40633
        if (bottom_prot instanceof TP) {
          bottom_prot.stop();
        }
        down(new Event(Event.STOP));
        stop_promise.getResult(5000);
        stopped=true;
    }

    /**
     * Not needed anymore, just left in here for backwards compatibility with JBoss AS
     * @deprecated
     */
    @Deprecated // GemStoneAddition
    public void flushEvents() {

    }

    @Override // GemStoneAddition
    public void stopInternal() {
        // do nothing, DON'T REMOVE !!!!
    }



    /*--------------------------- Transport interface ------------------------------*/

    public void send(Message msg) throws Exception {
        down(new Event(Event.MSG, msg));
    }

    public Object receive(long timeout) throws Exception {
        throw new Exception("ProtocolStack.receive(): not implemented !");
    }
    /*------------------------- End of  Transport interface ---------------------------*/





    /*--------------------------- Protocol functionality ------------------------------*/
    @Override // GemStoneAddition
    public String getName()  {return "ProtocolStack";}

    // GemStoneAddition - join shortcut using gossipserver's notion of who coordinator is
    public boolean hasTriedJoinShortcut() {
      boolean result = this.hasTriedJoinShortcut;
      this.hasTriedJoinShortcut = true;
      return result;
    }


    @Override // GemStoneAddition
    public void up(Event evt) {
        switch(evt.getType()) {
            case Event.START_OK:
                if(start_promise != null)
                    start_promise.setResult(evt.getArg());
                return;
            case Event.STOP_OK:
                if(stop_promise != null)
                    stop_promise.setResult(evt.getArg());
                return;
        }

        if(channel != null)
        {
          long sTime = 0;
          if (enableJgStackStats)
            sTime = nanoTime();
          
          channel.up(evt);

          if (enableJgStackStats)
            gfPeerFunctions.incjChannelUpTime(nanoTime() - sTime);
        }
    }




    @Override // GemStoneAddition
    public void down(Event evt) {
        if(top_prot != null)
        {
            long dTime = 0;
            if (enableJgStackStats)
                dTime = nanoTime();
          
            top_prot.receiveDownEvent(evt);

            if (enableJgStackStats)
              gfPeerFunctions.incjgChannelDownTime(nanoTime() - dTime);            
        }
        else {
          throw gfBasicFunctions.getDisconnectException(
              ExternalStrings.ProtocolStack_NO_DOWN_PROTOCOL_AVAILABLE_
              .toLocalizedString());
        }
    }



    @Override // GemStoneAddition
    protected void receiveUpEvent(Event evt) {
        up(evt);
    }



    /** Override with null functionality: we don't need any threads to be started ! */
    public void startWork() {}

    /** Override with null functionality: we don't need any threads to be started ! */
    public void stopWork()  {}


    /*----------------------- End of Protocol functionality ---------------------------*/




}
