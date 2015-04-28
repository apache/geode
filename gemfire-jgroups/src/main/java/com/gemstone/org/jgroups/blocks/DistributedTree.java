/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: DistributedTree.java,v 1.14 2005/11/10 20:54:01 belaban Exp $

package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.Util;

import java.io.Serializable;
import java.util.StringTokenizer;
import java.util.Vector;




/**
 * A tree-like structure that is replicated across several members. Updates will be multicast to all group
 * members reliably and in the same order.
 * @author Bela Ban
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
public class DistributedTree implements MessageListener, MembershipListener {
    Node root=null;
    final Vector listeners=new Vector();
    final Vector view_listeners=new Vector();
    final Vector members=new Vector();
    protected Channel channel=null;
    protected RpcDispatcher disp=null;
    String groupname="DistributedTreeGroup";
    String channel_properties="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=0):" +
            "PING(timeout=5000;num_initial_members=6):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "pbcast.NAKACK(gc_lag=5;retransmit_timeout=3000;trace=true):" +
            "UNICAST(timeout=5000):" +
            "FRAG(down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=false;print_local_addr=true):" +
            "pbcast.STATE_TRANSFER(trace=true)";
    final long state_timeout=5000;   // wait 5 secs max to obtain state

	/** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
	private boolean send_message = false;

    protected static final GemFireTracer log=GemFireTracer.getLog(DistributedTree.class);



    public interface DistributedTreeListener {
        void nodeAdded(String fqn, Serializable element);

        void nodeRemoved(String fqn);

        void nodeModified(String fqn, Serializable old_element, Serializable new_element);
    }


    public interface ViewListener {
        void viewChange(Vector new_mbrs, Vector old_mbrs);
    }


    public DistributedTree() {
    }


    public DistributedTree(String groupname, String channel_properties) {
        this.groupname=groupname;
        if(channel_properties != null)
            this.channel_properties=channel_properties;
    }

    /*
     * Uses a user-provided PullPushAdapter to create the dispatcher rather than a Channel. If id is non-null, it will be
     * used to register under that id. This is typically used when another building block is already using
     * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
     * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
     * first block created on PullPushAdapter.
     * @param adapter The PullPushAdapter which to use as underlying transport
     * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
     *           requests/responses for different building blocks on top of PullPushAdapter.
     * @param state_timeout Max number of milliseconds to wait until state is
     * retrieved
     */
    public DistributedTree(PullPushAdapter adapter, Serializable id, long state_timeout) 
        throws ChannelException {
        channel = (Channel)adapter.getTransport();
        disp=new RpcDispatcher(adapter, id, this, this, this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        boolean rc = channel.getState(null, state_timeout);
        if(rc) {
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedTree_STATE_WAS_RETRIEVED_SUCCESSFULLY);
        }
        else
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedTree_STATE_COULD_NOT_BE_RETRIEVED_MUST_BE_FIRST_MEMBER_IN_GROUP);
    }

    public Object getLocalAddress() {
        return channel != null? channel.getLocalAddress() : null;
    }

    public void setDeadlockDetection(boolean flag) {
        if(disp != null)
            disp.setDeadlockDetection(flag);
    }

    public void start() throws Exception {
        start(8000);
    }


    public void start(long timeout) throws Exception {
        if(channel != null) // already started
            return;
        channel=new JChannel(channel_properties);
        disp=new RpcDispatcher(channel, this, this, this);
        channel.connect(groupname);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        boolean rc=channel.getState(null, timeout);
        if(rc) {
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedTree_STATE_WAS_RETRIEVED_SUCCESSFULLY);
        }
        else
            if(log.isInfoEnabled()) log.info(ExternalStrings.DistributedTree_STATE_COULD_NOT_BE_RETRIEVED_MUST_BE_FIRST_MEMBER_IN_GROUP);
    }


    public void stop() {
        if(channel != null) {
            channel.close();
            disp.stop();
        }
        channel=null;
        disp=null;
    }


    public void addDistributedTreeListener(DistributedTreeListener listener) {
        if(!listeners.contains(listener))
            listeners.addElement(listener);
    }


    public void removeDistributedTreeListener(DistributedTreeListener listener) {
        listeners.removeElement(listener);
    }


    public void addViewListener(ViewListener listener) {
        if(!view_listeners.contains(listener))
            view_listeners.addElement(listener);
    }


    public void removeViewListener(ViewListener listener) {
        view_listeners.removeElement(listener);
    }


    public void add(String fqn) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_add", new Object[] {fqn}, new String[] {String.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_0, ex);
            }
        }
        else {
            _add(fqn);
        }
    }

    public void add(String fqn, Serializable element) {
        add(fqn, element, 0);
    }

    /** resets an existing node, useful after a merge when you want to tell other 
     *  members of your state, but do not wish to remove and then add as two separate calls */
    public void reset(String fqn, Serializable element) 
    {
        reset(fqn, element, 0);
    }

    public void remove(String fqn) {
        remove(fqn, 0);
    }

    public void add(String fqn, Serializable element, int timeout) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_add", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_0, ex);
            }
        }
        else {
            _add(fqn, element);
        }
    }

    /** resets an existing node, useful after a merge when you want to tell other 
     *  members of your state, but do not wish to remove and then add as two separate calls */
    public void reset(String fqn, Serializable element, int timeout) 
    {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_reset", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_0, ex);
            }
        }
        else {
            _add(fqn, element);
        }
    }

    public void remove(String fqn, int timeout) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
            	MethodCall call = new MethodCall("_remove", new Object[] {fqn}, new String[] {String.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_0, ex);
            }
        }
        else {
            _remove(fqn);
        }
    }


    public boolean exists(String fqn) {
        if(fqn == null)
            return false;
        return findNode(fqn) == null? false : true;
    }


    public Serializable get(String fqn) {
        Node n=null;

        if(fqn == null) return null;
        n=findNode(fqn);
        if(n != null) {
            return n.element;
        }
        return null;
    }


    public void set(String fqn, Serializable element) {
		set(fqn, element, 0);
    }

    public void set(String fqn, Serializable element, int timeout) {
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_set", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_0, ex);
            }
        }
        else {
            _set(fqn, element);
        }
    }


    /** Returns all children of a Node as strings */
    public Vector getChildrenNames(String fqn) {
        Vector ret=new Vector();
        Node n;

        if(fqn == null) return ret;
        n=findNode(fqn);
        if(n == null || n.children == null) return ret;
        for(int i=0; i < n.children.size(); i++)
            ret.addElement(((Node)n.children.elementAt(i)).name);
        return ret;
    }


    public String print() {
        StringBuffer sb=new StringBuffer();
        int indent=0;

        if(root == null)
            return "/";

        sb.append(root.print(indent));
        return sb.toString();
    }


    /** Returns all children of a Node as Nodes */
    Vector getChildren(String fqn) {
        Node n;

        if(fqn == null) return null;
        n=findNode(fqn);
        if(n == null) return null;
        return n.children;
    }

    /**
     * Returns the name of the group that the DistributedTree is connected to
     * @return String
     */
    public String  getGroupName()           {return groupname;}
	 	
    /**
     * Returns the Channel the DistributedTree is connected to 
     * @return Channel
     */
    public Channel getChannel()             {return channel;}

   /**
     * Returns the number of current members joined to the group
     * @return int
     */
    public int getGroupMembersNumber()			{return members.size();}




    /*--------------------- Callbacks --------------------------*/

    public void _add(String fqn) {
        _add(fqn, null);
    }


    public void _add(String fqn, Serializable element) {
        Node curr, n;
        StringTokenizer tok;
        String child_name;
        String tmp_fqn="";

        if(root == null) {
            root=new Node("/", null);
            notifyNodeAdded("/", null);
        }
        if(fqn == null)
            return;
        curr=root;
        tok=new StringTokenizer(fqn, "/");

        while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            tmp_fqn=tmp_fqn + '/' + child_name;
            n=curr.findChild(child_name);
            if(n == null) {
                n=new Node(child_name, null);
                curr.addChild(n);
                if(!tok.hasMoreTokens()) {
                    n.element=element;
                    notifyNodeAdded(tmp_fqn, element);
                    return;
                }
                else
                    notifyNodeAdded(tmp_fqn, null);
            }
            curr=n;
        }
        curr.element=element;
        notifyNodeModified(fqn, null, element);
    }


    public void _remove(String fqn) {
        Node curr, n;
        StringTokenizer tok;
        String child_name=null;

        if(fqn == null || root == null)
            return;
        curr=root;
        tok=new StringTokenizer(fqn, "/");

        while(tok.countTokens() > 1) {
            child_name=tok.nextToken();
            n=curr.findChild(child_name);
            if(n == null) // node does not exist
                return;
            curr=n;
        }
        try {
            child_name=tok.nextToken();
            if(child_name != null) {
                n=curr.removeChild(child_name);
                if(n != null)
                    notifyNodeRemoved(fqn);
            }
        }
        catch(Exception ex) {
        }
    }


    public void _set(String fqn, Serializable element) {
        Node n;
        Serializable old_el=null;

        if(fqn == null || element == null) return;
        n=findNode(fqn);
        if(n == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_NODE__0__NOT_FOUND, fqn);
            return;
        }
        old_el=n.element;
        n.element=element;
        notifyNodeModified(fqn, old_el, element);
    }

    /** similar to set, but does not error if node does not exist, but rather does an add instead */
    public void _reset(String fqn, Serializable element) {
        Node n;
        Serializable old_el=null;

        if(fqn == null || element == null) return;
        n=findNode(fqn);
        if(n == null) {
            _add(fqn, element);
            return;
        }
        old_el=n.element;
        n.element=element;
        notifyNodeModified(fqn, old_el, element);
    }

    /*----------------- End of  Callbacks ----------------------*/






    /*-------------------- State Exchange ----------------------*/

    public void receive(Message msg) {
    }

    /** Return a copy of the tree */
    public byte[] getState() {
        Object copy=root != null? root.copy() : null;
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_MARSHALLING_STATE__0, ex);
            return null;
        }
    }

    public void setState(byte[] data) {
        Object new_state;

        try {
            new_state=Util.objectFromByteBuffer(data);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_EXCEPTION_UNMARSHALLING_STATE__0, ex);
            return;
        }
        if(new_state == null) return;
        if(!(new_state instanceof Node)) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_OBJECT_IS_NOT_OF_TYPE_NODE);
            return;
        }
        root=((Node)new_state).copy();
    }



    /*------------------- Membership Changes ----------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        if(new_mbrs != null) {
            sendViewChangeNotifications(new_mbrs, members); // notifies observers (joined, left)
            members.removeAllElements();
            for(int i=0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
		//if size is bigger than one, there are more peers in the group
		//otherwise there is only one server.
        if(members.size() > 1) {
            send_message=true;
        }
        else {
            send_message=false;
        }
    }


    /** Called when a member is suspected */
    public void suspect(SuspectMember suspected_mbr) {
    }


    /** Block sending and receiving of messages until ViewAccepted is called */
    public void block() {
    }


    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    void sendViewChangeNotifications(Vector new_mbrs, Vector old_mbrs) {
        Vector joined, left;
        Object mbr;

        if(view_listeners.size() == 0 || old_mbrs == null || new_mbrs == null)
            return;


        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        joined=new Vector();
        for(int i=0; i < new_mbrs.size(); i++) {
            mbr=new_mbrs.elementAt(i);
            if(!old_mbrs.contains(mbr))
                joined.addElement(mbr);
        }


        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        left=new Vector();
        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr))
                left.addElement(mbr);
        }
        notifyViewChange(joined, left);
    }


    Node findNode(String fqn) {
        Node curr=root;
        StringTokenizer tok;
        String child_name;

        if(fqn == null || root == null) return null;
        if("/".equals(fqn) || "".equals(fqn))
            return root;

        tok=new StringTokenizer(fqn, "/");
        while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            curr=curr.findChild(child_name);
            if(curr == null) return null;
        }
        return curr;
    }


    void notifyNodeAdded(String fqn, Serializable element) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeAdded(fqn, element);
    }

    void notifyNodeRemoved(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeRemoved(fqn);
    }

    void notifyNodeModified(String fqn, Serializable old_element, Serializable new_element) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeModified(fqn, old_element, new_element);
    }

    /** Generates NodeAdded notifications for all nodes of the tree. This is called whenever the tree is
     initially retrieved (state transfer) */
    void notifyAllNodesCreated(Node curr, String tmp_fqn) {
        Node n;

        if(curr == null) return;
        if(curr.name == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_CURRNAME_IS_NULL);
            return;
        }

        if(curr.children != null) {
            for(int i=0; i < curr.children.size(); i++) {
                n=(Node)curr.children.elementAt(i);
                System.out.println("*** nodeCreated(): tmp_fqn is " + tmp_fqn);
                notifyNodeAdded(tmp_fqn, n.element);
                notifyAllNodesCreated(n, tmp_fqn + '/' + n.name);
            }
        }
    }


    void notifyViewChange(Vector new_mbrs, Vector old_mbrs) {
        for(int i=0; i < view_listeners.size(); i++)
            ((ViewListener)view_listeners.elementAt(i)).viewChange(new_mbrs, old_mbrs);
    }


    private static class Node implements Serializable {
        private static final long serialVersionUID = 2751686100384721286L;
        String name=null;
        Vector children=null;
        Serializable element=null;


        Node() {
        }

        Node(String name, Serializable element) {
            this.name=name;
            this.element=element;
        }


        void addChild(String relative_name, Serializable element) {
            if(relative_name == null)
                return;
            if(children == null)
                children=new Vector();
            else {
                if(!children.contains(relative_name))
                    children.addElement(new Node(relative_name, element));
            }
        }


        void addChild(Node n) {
            if(n == null) return;
            if(children == null)
                children=new Vector();
            if(!children.contains(n))
                children.addElement(n);
        }


        Node removeChild(String rel_name) {
            Node n=findChild(rel_name);

            if(n != null)
                children.removeElement(n);
            return n;
        }


        Node findChild(String relative_name) {
            Node child;

            if(children == null || relative_name == null)
                return null;
            for(int i=0; i < children.size(); i++) {
                child=(Node)children.elementAt(i);
                if(child.name == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.DistributedTree_CHILDNAME_IS_NULL_FOR__0, relative_name);
                    continue;
                }

                if(child.name.equals(relative_name))
                    return child;
            }

            return null;
        }


        @Override // GemStoneAddition
        public boolean equals(Object other) {
          if (!(other instanceof Node)) return false; // GemStoneAddition
            return /*other != null && */ ((Node)other).name != null && name != null && name.equals(((Node)other).name);
        }

        @Override // GemStoneAddition
        public int hashCode() { // GemStoneAddition
          return 0; // TODO more efficient implementation :-)
        }

        Node copy() {
            Node ret=new Node(name, element);

            if(children != null)
                ret.children=(Vector)children.clone();
            return ret;
        }


        String print(int indent) {
            StringBuffer sb=new StringBuffer();
            boolean is_root=name != null && "/".equals(name);

            for(int i=0; i < indent; i++)
                sb.append(' ');
            if(!is_root) {
                if(name == null)
                    sb.append("/<unnamed>");
                else {
                    sb.append('/' + name);
                    // if(element != null) sb.append(" --> " + element);
                }
            }
            sb.append('\n');
            if(children != null) {
                if(is_root)
                    indent=0;
                else
                    indent+=4;
                for(int i=0; i < children.size(); i++)
                    sb.append(((Node)children.elementAt(i)).print(indent));
            }
            return sb.toString();
        }


        @Override // GemStoneAddition
        public String toString() {
            if(element != null)
                return "[name: " + name + ", element: " + element + ']';
            else
                return "[name: " + name + ']';
        }

    }


}


