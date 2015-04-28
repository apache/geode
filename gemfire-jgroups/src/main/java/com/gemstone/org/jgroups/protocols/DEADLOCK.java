/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DEADLOCK.java,v 1.7 2005/08/08 12:45:42 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.blocks.GroupRequest;
import com.gemstone.org.jgroups.blocks.MethodCall;
import com.gemstone.org.jgroups.stack.RpcProtocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.RspList;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;




class MyFrame extends Frame  {
    private static final long serialVersionUID = -5091554906224946278L;
   
    final List        list=new List();
    final Label       result=new Label("Result:          ");
    final Button      send=new Button("Send Request");
    final Button quit=new Button("Quit");
    final Panel       button_panel=new Panel();
    final Panel       main_panel=new Panel();
    DEADLOCK    deadlock=null;
    Vector      members=null;
    
    
    MyFrame(String title, DEADLOCK deadlock) {
	this.deadlock=deadlock;
	setSize(300, 200);
	setTitle(title);
	setBackground(Color.white);
	setFont(new Font("Helvetica", Font.PLAIN, 12));
	setLayout(new BorderLayout());
	main_panel.setLayout(new GridLayout(0, 2));
	main_panel.add(result);
	main_panel.add(list);
	button_panel.add(send);
	button_panel.add(quit);
	add("Center", main_panel);
	add("South", button_panel);
	addEventHandlers();
    }


    void addEventHandlers() {

	quit.addActionListener(
			       new ActionListener() {
	    public void actionPerformed(ActionEvent e) {dispose();}
	});

	send.addActionListener(
			       new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		Address dest;
		int     res;
		int     index=-1;
		
		index=list.getSelectedIndex();
		if(index == -1)
		    return;
		dest=members != null ? (Address)members.elementAt(index) : null;
		if(dest != null) {
		    res=deadlock.sendRequest(dest);
		    setResult(res);
		}
	    }
	});

	
    }


    void setResult(int res) {
	result.setText("Result: " + res);
    }

    void setMembers(Vector members) {
	list.removeAll();
	for(int i=0; i < members.size(); i++)
	    list.add(members.elementAt(i).toString());
	this.members=members;
    }

}





/**
   Tests the deadlock detection mechanism of RequestCorrelator.
 */
public class DEADLOCK extends RpcProtocol  {
    MyFrame  frame=null;
    



    @Override // GemStoneAddition
    public String  getName() {return "DEADLOCK";}


    @Override // GemStoneAddition
    public void start() throws Exception {
        super.start();
        if(_corr != null)
            _corr.setDeadlockDetection(true);
        else
            log.error(ExternalStrings.DEADLOCK_CANNOT_SET_DEADLOCK_DETECTION_IN_CORR_AS_IT_IS_NULL_);
        frame=new MyFrame(getName(), this);
        frame.show();
    }

    @Override // GemStoneAddition
    public void stop() {
        super.stop();
        if(frame != null) {
            frame.dispose();
            frame=null;
        }
    }
    

    public int sendRequest(Address dest) {
	Object retval;
	
	try {
	    System.out.println("--> getCombinedResults() to " + dest);
	    retval=callRemoteMethod(dest, "getCombinedResults", GroupRequest.GET_FIRST, 0);
	}
	catch(Exception e) {
	    return -1;
	}
	if(retval != null && retval instanceof Integer)
	    return ((Integer)retval).intValue();
	return -1;
    }


    /* ------------------------- Request handler methods ----------------------------- */


    /** Mcasts getResult() to all members (including itself). Returns the sum of all results. */
    public int getCombinedResults() {
	RspList  rsp_list;
	Vector   results;
	int      retval=0;

	System.out.println("<-- getCombinedResults()");

	System.out.println("--> getResult() to " + members);
	MethodCall call = new MethodCall("getResult", new Object[] {}, new String[] {});
	rsp_list=callRemoteMethods(members, call, GroupRequest.GET_ALL, 0);
	results=rsp_list.getResults();
	for(int i=0; i < results.size(); i++)
	    retval+=((Integer)results.elementAt(i)).intValue();
	return retval;
    }


    /** Returns a random integer value between 1 and 10 */
    public static int getResult() {
        System.out.println("<-- getResult()");
        return (int)((Math.random() * 10) % 10) + 1;
    }


    /* --------------------- End of Request handler methods -------------------------- */




    
    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p>
     * <b>Do not use <code>PassUp</code> in this method as the event is passed up
     * by default by the superclass after this method returns !</b>
     * @return boolean Defaults to true. If false, event will not be passed up the stack.
     */
    @Override // GemStoneAddition
    public boolean handleUpEvent(Event evt) {
	switch(evt.getType()) {

	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:
	    Vector new_members=((View)evt.getArg()).getMembers();
	    synchronized(members) {
		members.removeAllElements();
		if(new_members != null && new_members.size() > 0)
		    for(int i=0; i < new_members.size(); i++)
			members.addElement(new_members.elementAt(i));
	    }
	    frame.setMembers(members);
	    break;


	case Event.SET_LOCAL_ADDRESS:
	    frame.setTitle(frame.getTitle() + ": " + evt.getArg().toString());
	    break;
	    
	}

	return true;
    }


    /**
       <b>Callback</b>. Called by superclass when event may be handled.<p>
       <b>Do not use <code>PassDown</code> in this method as the event is passed down
       by default by the superclass after this method returns !</b>
       @return boolean Defaults to true. If false, event will not be passed down the stack.
    */
    @Override // GemStoneAddition
    public boolean handleDownEvent(Event evt) {
	switch(evt.getType()) {
	case Event.TMP_VIEW:
	case Event.VIEW_CHANGE:
	    Vector new_members=((View)evt.getArg()).getMembers();
	    synchronized(members) {
		members.removeAllElements();
		if(new_members != null && new_members.size() > 0)
		    for(int i=0; i < new_members.size(); i++)
			members.addElement(new_members.elementAt(i));
	    }
	    System.out.println("Setting members");
	    frame.setMembers(members);
	    System.out.println("done");
	    break;
	}
	return true;
    }



}
