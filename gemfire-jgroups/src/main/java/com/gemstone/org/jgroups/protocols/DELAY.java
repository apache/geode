/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DELAY.java,v 1.7 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Util;

import java.util.Properties;


/**
 * Delays incoming/outgoing messages by a random number of milliseconds (range between 0 and n
 * where n is determined by the user). Incoming messages can be delayed independently from
 * outgoing messages (or not delayed at all).<p>
 * This protocol should be inserted directly above the bottommost protocol (e.g. UDP).
 */

public class DELAY extends Protocol  {
    int in_delay=0, out_delay=0;

    /**
     * All protocol names have to be unique !
     */
    @Override // GemStoneAddition
    public String getName() {
        return "DELAY";
    }


    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("in_delay");
        if(str != null) {
            in_delay=Integer.parseInt(str);
            props.remove("in_delay");
        }

        str=props.getProperty("out_delay");
        if(str != null) {
            out_delay=Integer.parseInt(str);
            props.remove("out_delay");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.DELAY_DELAYSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition
    public void up(Event evt) {
        int delay=in_delay > 0 ? computeDelay(in_delay) : 0;


        switch(evt.getType()) {
            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info(ExternalStrings.DELAY_DELAYING_INCOMING_MESSAGE_FOR__0__MILLISECONDS, delay);
                try { // GemStoneAddition
                  Util.sleep(delay);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt(); // just propagate
                }
                break;
        }

        passUp(evt);            // Pass up to the layer above us
    }


    @Override // GemStoneAddition
    public void down(Event evt) {
        int delay=out_delay > 0 ? computeDelay(out_delay) : 0;

        switch(evt.getType()) {

            case Event.MSG:         // Only delay messages, not other events !

                if(log.isInfoEnabled()) log.info(ExternalStrings.DELAY_DELAYING_OUTGOING_MESSAGE_FOR__0__MILLISECONDS, delay);
                try { // GemStoneAddition
                  Util.sleep(delay);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt(); // just propagate
                }
                break;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    /**
     * Compute a random number between 0 and n
     */
    static int computeDelay(int n) {
        return (int)((Math.random() * 1000000) % n);
    }


}
