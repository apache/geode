/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Debugger.java,v 1.5 2005/08/22 08:31:25 belaban Exp $

package com.gemstone.org.jgroups.debug;


import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolStack;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.Vector;


/**
 * The Debugger displays a graphical view of the protocol stack by showing all the protocols and
 * the events in them.
 *
 * @author Bela Ban
 */
public class Debugger extends JFrame {
    private static final long serialVersionUID = 1264635453541531112L;
    JChannel channel=null;
    Vector prots=new Vector();
    JButton b1, b2;
    JPanel button_panel;
    JTable table;
    DefaultTableModel table_model;
    JScrollPane scroll_pane;
    public static final Font helvetica_12=new Font("Helvetica", Font.PLAIN, 12);;
    public boolean cummulative=false; // shows added up/down events instead of up/down queue_size



    public Debugger() {
        super("Debugger Window");
    }


    public Debugger(JChannel channel) {
        super("Debugger Window");
        this.channel=channel;
    }


    public Debugger(JChannel channel, String name) {
        super(name);
        this.channel=channel;
    }

    public Debugger(JChannel channel, boolean cummulative) {
        super("Debugger Window");
        this.channel=channel;
        this.cummulative=cummulative;
    }


    public Debugger(JChannel channel, boolean cummulative, String name) {
        super(name);
        this.channel=channel;
        this.cummulative=cummulative;
    }


    public void setChannel(JChannel channel) {
        this.channel=channel;
    }


    public void start() {
        Protocol prot;
        ProtocolStack stack;
        ProtocolView view=null;

        if(channel == null) return;
        stack=channel.getProtocolStack();
        prots=stack.getProtocols();

        setBounds(new Rectangle(30, 30, 300, 300));
        table_model=new DefaultTableModel();
        table=new JTable(table_model);
        table.setFont(helvetica_12);
        scroll_pane=new JScrollPane(table);
        table_model.setColumnIdentifiers(new String[]{"Index", "Name", "up", "down"});

        getContentPane().add(scroll_pane);
        show();

        for(int i=0; i < prots.size(); i++) {
            prot=(Protocol)prots.elementAt(i);
            view=new ProtocolView(prot, table_model, i, cummulative);
            prot.setObserver(view);
            table_model.insertRow(i, new Object[]{"" + (i + 1),
                                                  prot.getName(), prot.getUpQueue().size() + "",
                                                  prot.getDownQueue().size() + "", "0", "0"});

            //prot_view=CreateProtocolView(prot.getName());
            //if(prot_view != null) {
            //JFrame f=new JFrame("New View for " + prot.GetName());
            //f.getContentPane().add(prot_view);
            //f.show();
            //}
        }
    }

    public void stop() {
        Protocol prot;
        ProtocolStack stack;

        if(channel == null) return;
        stack=channel.getProtocolStack();
        prots=stack.getProtocols();

        for(int i=0; i < prots.size(); i++) {
            prot=(Protocol)prots.elementAt(i);
            prot.setObserver(null);
        }
        dispose();
    }



//    public static void main(String[] args) {
//        Debugger d=new Debugger();
//        d.start();
//    }
}

