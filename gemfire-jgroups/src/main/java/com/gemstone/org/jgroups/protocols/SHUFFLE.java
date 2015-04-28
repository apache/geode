/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.util.*;



/**
 * This layer shuffles upcoming messages, put it just above your bottom layer.
 * If you system sends less than 2 messages per sec you can notice a latency due
 * to this layer.
 *
 * @author Gianluca Collot
 *
 */

public class SHUFFLE extends Protocol implements Runnable {

    String       name="SHUFFLE";
    final List         messages;
    Thread       messagesHandler; // GemStoneAddition - accesses synchronized on this

    public SHUFFLE() {
        messages = Collections.synchronizedList(new ArrayList());
    }

    @Override // GemStoneAddition
    public String getName() {
        return name;
    }

    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String     str;

        super.setProperties(props);
        str=props.getProperty("name");
        if(str != null) {
            name=str;
            props.remove("name");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.SHUFFLE_DUMMYSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }

    /**
     * Adds upcoming messages to the <code>messages List<\code> where the <code>messagesHandler<\code>
     * retrieves them.
     */

    @Override // GemStoneAddition
    public void up(Event evt) {
        Message msg;

        switch (evt.getType()) {

	case Event.MSG:
            msg=(Message)evt.getArg();
            // Do something with the event, e.g. extract the message and remove a header.
            // Optionally pass up
            messages.add(msg);
            return;
        }

        passUp(evt);            // Pass up to the layer above us
    }




    /**
     * Starts the <code>messagesHandler<\code>
     */
    @Override // GemStoneAddition
    synchronized /* GemStoneAddition */ public void start() throws Exception {
        messagesHandler = new Thread(this,"MessagesHandler");
        messagesHandler.setDaemon(true);
        messagesHandler.start();
    }

    /**
     * Stops the messagesHandler
     */
    @Override // GemStoneAddition
    synchronized /* GemStoneAddition */ public void stop() {
        Thread tmp = messagesHandler;
        messagesHandler = null;
        if (tmp != null) { // GemStoneAddition
          tmp.interrupt(); // GemStoneAddition
        try {
            tmp.join();
        } catch (InterruptedException ex) {ex.printStackTrace(); Thread.currentThread().interrupt(); /*GemStoneAddition*/}
        }
    }

    /**
     * Removes a random chosen message from the <code>messages List<\code> if there
     * are less than 10 messages in the List it waits some time to ensure to chose from
     * a set of messages > 1.
     */

    public void run() {
        Message msg;
        for (;;) { // GemStoneAddition -- remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
            if ( messages.size() > 0 ) {
                msg = (Message) messages.remove(rnd(messages.size()));
                passUp(new Event(Event.MSG,msg));
            }
            if (messages.size() < 5) {
                try {
                    Thread.sleep(300); // @todo make this time user configurable
                }
                catch (InterruptedException ex) { // GemStoneAddition
                  Thread.currentThread().interrupt(); // GemStoneAddition
                  return; // GemStoneAddition - exit loop and thread
//                    ex.printStackTrace();
                }
            }
        }// while
        // PassUp remaining messages
        Iterator iter = messages.iterator();
        while (iter.hasNext()) {
            msg = (Message) iter.next();
            passUp(new Event(Event.MSG,msg));
        }
    }

    // random integer between 0 and n-1
    int rnd(int n) { return (int)(Math.random()*n); }

}
