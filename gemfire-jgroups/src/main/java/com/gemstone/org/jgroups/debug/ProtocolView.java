/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ProtocolView.java,v 1.2 2004/09/23 16:29:16 belaban Exp $

package com.gemstone.org.jgroups.debug;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolObserver;



/**
 * Graphical view of a protocol instance
 * @author  Bela Ban, created July 22 2000
 */
public class ProtocolView implements ProtocolObserver {
    final DefaultTableModel  model;
    int                my_index=-1;
    Protocol           prot=null;
//    String             prot_name=null;GemStoneAddition
    final JButton            down_label=new JButton("0");
    final JButton up_label=new JButton("0");
    boolean            cummulative=false;
    long               tot_up=0, tot_down=0;



    public ProtocolView(Protocol p, DefaultTableModel model, int my_index) {
	prot=p; /* prot_name=p.getName(); GemStoneAddition*/ this.model=model; this.my_index=my_index;
    }


    public ProtocolView(Protocol p, DefaultTableModel model, int my_index, boolean cummulative) {
	prot=p; /* prot_name=p.getName(); GemStoneAddition*/ this.model=model; this.my_index=my_index; this.cummulative=cummulative;
    }

    


    /* ----------------------- ProtocolObserver interface ----------------------- */
    public void setProtocol(Protocol prot) {
	this.prot=prot;
    }

    
    public boolean up(Event evt, int num_evts) {
	tot_up++;
	if(cummulative)
	    model.setValueAt("" + tot_up, my_index, 2);
	else
	    model.setValueAt("" + num_evts, my_index, 2);
	return true;
    }


    public boolean passUp(Event evt) {
	return true;
    }
    
    
    public boolean down(Event evt, int num_evts) {
	tot_down++;
	if(cummulative)
	    model.setValueAt("" + tot_down, my_index, 3);
	else
	    model.setValueAt("" + num_evts, my_index, 3);
	return true;
    }

    public boolean passDown(Event evt) {
	return true;
    }

    /* ------------------- End of ProtocolObserver interface ---------------------- */



    @Override // GemStoneAddition
    public String toString() {
	return prot != null ? prot.getName() : "<n|a>";
    }


}
