/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: VIEW_ENFORCER.java,v 1.4 2005/05/30 14:31:24 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.util.Vector;


/**
 * Used by a client until it becomes a member: all up messages are discarded until a VIEW_CHANGE
 * is encountered. From then on, this layer just acts as a pass-through layer. Later, we may
 * add some functionality that checks for the VIDs of messages and discards messages accordingly.
 */

public class VIEW_ENFORCER extends Protocol  {
    Address local_addr=null;
    boolean is_member=false;


    /**
     * All protocol names have to be unique !
     */
    @Override // GemStoneAddition
    public String getName() {
        return "VIEW_ENFORCER";
    }


    @Override // GemStoneAddition
    public void up(Event evt) {

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                if(is_member)       // pass the view change up if we are already a member of the group
                    break;

                Vector new_members=((View)evt.getArg()).getMembers();
                if(new_members == null || new_members.size() == 0)
                    break;
                if(local_addr == null) {
                    log.error("VIEW_ENFORCER.up(VIEW_CHANGE): local address is null; cannot check " +
                            "whether I'm a member of the group; discarding view change");
                    return;
                }

                if(new_members.contains(local_addr))
                    is_member=true;
                else
                    return;

                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;


            case Event.MSG:
                if(!is_member) {    // drop message if we are not yet member of the group
                    if(log.isInfoEnabled()) log.info(ExternalStrings.VIEW_ENFORCER_DROPPING_MESSAGE__0, (Object)evt.getArg());
                    return;
                }
                break;

        }
        passUp(evt);            // Pass up to the layer above us
    }


}
