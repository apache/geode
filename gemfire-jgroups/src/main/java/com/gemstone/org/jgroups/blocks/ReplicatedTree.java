/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReplicatedTree.java,v 1.11 2005/11/10 20:54:01 belaban Exp $

package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.util.ExternalStrings;
//import com.gemstone.org.jgroups.jmx.JmxConfigurator;
import com.gemstone.org.jgroups.util.Queue;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.management.MBeanServerFactory;


//import javax.management.MBeanServer;
import java.io.Serializable;
import java.util.*;




/**
 * A tree-like structure that is replicated across several members. Updates will be multicast to all group
 * members reliably and in the same order.
 * @author Bela Ban Jan 17 2002
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
@SuppressFBWarnings(value="CN_IMPLEMENTS_CLONE_BUT_NOT_CLONEABLE", justification="GemFire does not use this class")
public class ReplicatedTree implements Runnable, MessageListener, MembershipListener {
    public static final String SEPARATOR="/";
    final static int INDENT=4;
    Node root=new Node(SEPARATOR, SEPARATOR, null, null);
    final Vector listeners=new Vector();
    final Queue request_queue=new Queue();
    Thread request_handler=null; // GemStoneAddition -- accesses synchronized on this
    JChannel channel=null;
    PullPushAdapter adapter=null;
    String groupname="ReplicatedTree-Group";
    final Vector members=new Vector();
    long state_fetch_timeout=10000;
//    boolean jmx=false; GemStoneAddition

    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());


    /** Whether or not to use remote calls. If false, all methods will be invoked directly on this
     instance rather than sending a message to all replicas and only then invoking the method.
     Useful for testing */
    boolean remote_calls=true;
    String props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=false;print_local_addr=true):" +
            "pbcast.STATE_TRANSFER";
    // "PERF(details=true)";

	/** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
	private boolean send_message = false;



    public interface ReplicatedTreeListener {
        void nodeAdded(String fqn);

        void nodeRemoved(String fqn);

        void nodeModified(String fqn);

        void viewChange(View new_view);  // might be MergeView after merging
    }


    /**
     * Creates a channel with the given properties. Connects to the channel, then creates a PullPushAdapter
     * and starts it
     */
    public ReplicatedTree(String groupname, String props, long state_fetch_timeout) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.connect(this.groupname);
        start();
    }

    public ReplicatedTree(String groupname, String props, long state_fetch_timeout, boolean jmx) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
//        this.jmx=jmx; GemStoneAddition
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.connect(this.groupname);
        if(jmx) {
            ArrayList servers=MBeanServerFactory.findMBeanServer(null);
            if(servers == null || servers.size() == 0) {
                throw new Exception("No MBeanServers found;" +
                                    "\nJmxTest needs to be run with an MBeanServer present, or inside JDK 5");
            }
//            MBeanServer server=(MBeanServer)servers.get(0); GemStoneAddition
//            JmxConfigurator.registerChannel(channel, server, "JGroups:channel=" + channel.getChannelName() , true);
        }
        start();
    }

    public ReplicatedTree() {
    }


    /**
     * Expects an already connected channel. Creates a PullPushAdapter and starts it
     */
    public ReplicatedTree(JChannel channel) throws Exception {
        this.channel=channel;
        start();
    }


    public void setRemoteCalls(boolean flag) {
        remote_calls=flag;
    }

    public void setRootNode(Node n) {
        root=n;
    }

    public Address getLocalAddress() {
        return channel != null? channel.getLocalAddress() : null;
    }

    public Vector getMembers() {
        return members;
    }


    /**
     * Fetch the group state from the current coordinator. If successful, this will trigger setState().
     */
    public void fetchState(long timeout) throws ChannelClosedException, ChannelNotConnectedException {
        boolean rc=channel.getState(null, timeout);
        if(rc)
            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedTree_STATE_WAS_RETRIEVED_SUCCESSFULLY);
        else
            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedTree_STATE_COULD_NOT_BE_RETRIEVED_FIRST_MEMBER);
    }


    public void addReplicatedTreeListener(ReplicatedTreeListener listener) {
        if(!listeners.contains(listener))
            listeners.addElement(listener);
    }


    public void removeReplicatedTreeListener(ReplicatedTreeListener listener) {
        listeners.removeElement(listener);
    }


    public synchronized /* GemStoneAddition */ void start() throws Exception {
        if(request_handler == null) {
            request_handler=new Thread(this, "ReplicatedTree.RequestHandler thread");
            request_handler.setDaemon(true);
            request_handler.start();
        }
        adapter=new PullPushAdapter(channel, this, this);
        adapter.setListener(this);
        channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
        boolean rc=channel.getState(null, state_fetch_timeout);
        if(rc)
            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedTree_STATE_WAS_RETRIEVED_SUCCESSFULLY);
        else
            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedTree_STATE_COULD_NOT_BE_RETRIEVED_FIRST_MEMBER);
    }


    public synchronized /* GemStoneAddition */ void stop() {
        if(request_handler != null && request_handler.isAlive()) {
            request_queue.close(true);
            request_handler.interrupt(); // GemStoneAddition
            request_handler=null;
        }

        request_handler=null;
        if(channel != null) {
            channel.close();
        }
        if(adapter != null) {
            adapter.stop();
            adapter=null;
        }
        channel=null;
    }


    /**
     * Adds a new node to the tree and sets its data. If the node doesn not yet exist, it will be created.
     * Also, parent nodes will be created if not existent. If the node already has data, then the new data
     * will override the old one. If the node already existed, a nodeModified() notification will be generated.
     * Otherwise a nodeCreated() motification will be emitted.
     * @param fqn The fully qualified name of the new node
     * @param data The new data. May be null if no data should be set in the node.
     */
    public void put(String fqn, HashMap data) {
        if(!remote_calls) {
            _put(fqn, data);
            return;
        }

		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_CHANNEL_IS_NULL_CANNOT_BROADCAST_PUT_REQUEST);
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.PUT, fqn, data)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_FAILURE_BCASTING_PUT_REQUEST__0, ex);
            }
        }
        else {
            _put(fqn, data);
        }
    }


    /**
     * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node
     * already existed, a nodeModified() notification will be generated. Otherwise a
     * nodeCreated() motification will be emitted.
     * @param fqn The fully qualified name of the node
     * @param key The key
     * @param value The value
     */
    public void put(String fqn, String key, Object value) {
        if(!remote_calls) {
            _put(fqn, key, value);
            return;
        }

        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {

            if(channel == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_CHANNEL_IS_NULL_CANNOT_BROADCAST_PUT_REQUEST);
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.PUT, fqn, key, value)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_FAILURE_BCASTING_PUT_REQUEST__0, ex);
            }
        }
        else {
            _put(fqn, key, value);
        }
    }


    /**
     * Removes the node from the tree.
     * @param fqn The fully qualified name of the node.
     */
    public void remove(String fqn) {
        if(!remote_calls) {
            _remove(fqn);
            return;
        }
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_CHANNEL_IS_NULL_CANNOT_BROADCAST_REMOVE_REQUEST);
                return;
            }
            try {
                channel.send(
                        new Message(null, null, new Request(Request.REMOVE, fqn)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_FAILURE_BCASTING_REMOVE_REQUEST__0, ex);
            }
        }
        else {
            _remove(fqn);
        }
    }


    /**
     * Removes <code>key</code> from the node's hashmap
     * @param fqn The fullly qualified name of the node
     * @param key The key to be removed
     */
    public void remove(String fqn, String key) {
        if(!remote_calls) {
            _remove(fqn, key);
            return;
        }
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_CHANNEL_IS_NULL_CANNOT_BROADCAST_REMOVE_REQUEST);
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.REMOVE, fqn, key)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_FAILURE_BCASTING_REMOVE_REQUEST__0, ex);
            }
        }
        else {
            _remove(fqn, key);
        }
    }


    /**
     * Checks whether a given node exists in the tree
     * @param fqn The fully qualified name of the node
     * @return boolean Whether or not the node exists
     */
    public boolean exists(String fqn) {
        if(fqn == null) return false;
        return findNode(fqn) != null? true : false;
    }


    /**
     * Gets the keys of the <code>data</code> map. Returns all keys as Strings. Returns null if node
     * does not exist.
     * @param fqn The fully qualified name of the node
     * @return Set A set of keys (as Strings)
     */
    public Set getKeys(String fqn) {
        Node n=findNode(fqn);
        Map data;

        if(n == null) return null;
        data=n.getData();
        if(data == null) return null;
        return data.keySet();
    }


    /**
     * Finds a node given its name and returns the value associated with a given key in its <code>data</code>
     * map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
     * @param fqn The fully qualified name of the node.
     * @param key The key.
     */
    public Object get(String fqn, String key) {
        Node n=findNode(fqn);

        if(n == null) return null;
        return n.getData(key);
    }


    /**
     * Returns the data hashmap for a given node. This method can only be used by callers that are inside
     * the same package. The reason is that callers must not modify the return value, as these modifications
     * would not be replicated, thus rendering the replicas inconsistent.
     * @param fqn The fully qualified name of the node
     * @return HashMap The data hashmap for the given node
     */
    HashMap get(String fqn) {
        Node n=findNode(fqn);

        if(n == null) return null;
        return n.getData();
    }


    /**
     * Prints a representation of the node defined by <code>fqn</code>. Output includes name, fqn and
     * data.
     */
    public String print(String fqn) {
        Node n=findNode(fqn);
        if(n == null) return null;
        return n.toString();
    }


    /**
     * Returns all children of a given node
     * @param fqn The fully qualified name of the node
     * @return Set A list of child names (as Strings)
     */
    public Set getChildrenNames(String fqn) {
        Node n=findNode(fqn);
        Map m;

        if(n == null) return null;
        m=n.getChildren();
        if(m != null)
            return m.keySet();
        else
            return null;
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        int indent=0;
        Map children;

        children=root.getChildren();
        if(children != null && children.size() > 0) {
            Collection nodes=children.values();
            for(Iterator it=nodes.iterator(); it.hasNext();) {
                ((Node)it.next()).print(sb, indent);
                sb.append('\n');
            }
        }
        else
            sb.append(SEPARATOR);
        return sb.toString();
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




    /* --------------------- Callbacks -------------------------- */


    public void _put(String fqn, HashMap data) {
        Node n;
        StringHolder child_name=new StringHolder();
        boolean child_exists=false;

        if(fqn == null) return;
        n=findParentNode(fqn, child_name, true); // create all nodes if they don't exist
        if(child_name.getValue() != null) {
            child_exists=n.childExists(child_name.getValue());
            n.createChild(child_name.getValue(), fqn, n, data);
        }
        else {
            child_exists=true;
            n.setData(data);
        }
        if(child_exists)
            notifyNodeModified(fqn);
        else
            notifyNodeAdded(fqn);
    }


    public void _put(String fqn, String key, Object value) {
        Node n;
        StringHolder child_name=new StringHolder();
        boolean child_exists=false;

        if(fqn == null || key == null || value == null) return;
        n=findParentNode(fqn, child_name, true);
        if(child_name.getValue() != null) {
            child_exists=n.childExists(child_name.getValue());
            n.createChild(child_name.getValue(), fqn, n, key, value);
        }
        else {
            child_exists=true;
            n.setData(key, value);
        }
        if(child_exists)
            notifyNodeModified(fqn);
        else
            notifyNodeAdded(fqn);
    }


    public void _remove(String fqn) {
        Node n;
        StringHolder child_name=new StringHolder();

        if(fqn == null) return;
        if(fqn.equals(SEPARATOR)) {
            root.removeAll();
            notifyNodeRemoved(fqn);
            return;
        }
        n=findParentNode(fqn, child_name, false);
        if(n == null) return;
        n.removeChild(child_name.getValue(), fqn);
        notifyNodeRemoved(fqn);
    }


    public void _remove(String fqn, String key) {
        Node n;

        if(fqn == null || key == null) return;
        n=findNode(fqn);
        if(n != null)
            n.removeData(key);
    }


    public void _removeData(String fqn) {
        Node n;

        if(fqn == null) return;
        n=findNode(fqn);
        if(n != null)
            n.removeData();
    }


    /* ----------------- End of  Callbacks ---------------------- */






    /*-------------------- MessageListener ----------------------*/

    /** Callback. Process the contents of the message; typically an _add() or _set() request */
    public void receive(Message msg) {
        Request req=null;

        if(msg == null || msg.getLength() == 0)
            return;
        try {
            req=(Request)msg.getObject();
            request_queue.add(req);
        }
        catch(QueueClosedException queue_closed_ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_REQUEST_QUEUE_IS_NULL);
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_FAILED_UNMARSHALLING_REQUEST__0, ex);
            return;
        }
    }

    /** Return a copy of the current cache (tree) */
    public byte[] getState() {
        try {
            return Util.objectToByteBuffer(root.clone());
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_EXCEPTION_RETURNING_CACHE__0, ex);
            return null;
        }
    }

    /** Set the cache (tree) to this value */
    public void setState(byte[] new_state) {
        Node new_root=null;
        Object obj;

        if(new_state == null) {
            if(log.isInfoEnabled()) log.info(ExternalStrings.ReplicatedTree_NEW_CACHE_IS_NULL);
            return;
        }
        try {
            obj=Util.objectFromByteBuffer(new_state);
            new_root=(Node)((Node)obj).clone();
            root=new_root;
            notifyAllNodesCreated(root);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_COULD_NOT_SET_CACHE__0, ex);
        }
    }

    /*-------------------- End of MessageListener ----------------------*/





    /*----------------------- MembershipListener ------------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        // todo: if MergeView, fetch and reconcile state from coordinator
        // actually maybe this is best left up to the application ? we just notify them and let
        // the appl handle it ?

        if(new_mbrs != null) {
            notifyViewChange(new_view);
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
        ;
    }


    /** Block sending and receiving of messages until viewAccepted() is called */
    public void block() {
    }

    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
    /*------------------- End of MembershipListener ----------------------*/



    /** Request handler thread */
    public void run() {
        Request req;
        String fqn=null;

        for (;;) { // GemStoneAddition remove coding anti-pattern
          if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
            try {
                req=(Request)request_queue.remove(0);
                fqn=req.fqn;
                switch(req.type) {
                    case Request.PUT:
                        if(req.key != null && req.value != null)
                            _put(fqn, req.key, req.value);
                        else
                            _put(fqn, req.data);
                        break;
                    case Request.REMOVE:
                        if(req.key != null)
                            _remove(fqn, req.key);
                        else
                            _remove(fqn);
                        break;
                    default:
                        if(log.isErrorEnabled()) log.error(ExternalStrings.ReplicatedTree_TYPE__0__UNKNOWN, req.type);
                        break;
                }
            }
            catch(QueueClosedException queue_closed_ex) {
//                request_handler=null; GemStoneAddition
                break;
            }
            catch(Throwable other_ex) {
                if(log.isWarnEnabled()) log.warn("exception processing request: " + other_ex);
            }
        }
    }


    /**
     * Find the node just <em>above</em> the one indicated by <code>fqn</code>. This is needed in many cases,
     * e.g. to add a new node or remove an existing node.
     * @param fqn The fully qualified name of the node.
     * @param child_name Will be filled with the name of the child when this method returns. The child name
     *                   is the last relative name of the <code>fqn</code>, e.g. in "/a/b/c" it would be "c".
     * @param create_if_not_exists Create parent nodes along the way if they don't exist. Otherwise, this method
     *                             will return when a node cannot be found.
     */
    Node findParentNode(String fqn, StringHolder child_name, boolean create_if_not_exists) {
        Node curr=root, node;
        StringTokenizer tok;
        String name;
        StringBuffer sb=null;

        if(fqn == null || fqn.equals(SEPARATOR) || "".equals(fqn))
            return curr;

        sb=new StringBuffer();
        tok=new StringTokenizer(fqn, SEPARATOR);
        while(tok.countTokens() > 1) {
            name=tok.nextToken();
            sb.append(SEPARATOR).append(name);
            node=curr.getChild(name);
            if(node == null && create_if_not_exists)
                node=curr.createChild(name, sb.toString(), null, null);
            if(node == null)
                return null;
            else
                curr=node;
        }

        if(tok.countTokens() > 0 && child_name != null)
            child_name.setValue(tok.nextToken());
        return curr;
    }


    /**
     * Returns the node at fqn. This method should not be used by clients (therefore it is package-private):
     * it is only used internally (for navigation). C++ 'friend' would come in handy here...
     * @param fqn The fully qualified name of the node
     * @return Node The node at fqn
     */
    Node findNode(String fqn) {
        StringHolder sh=new StringHolder();
        Node n=findParentNode(fqn, sh, false);
        String child_name=sh.getValue();

        if(fqn == null || fqn.equals(SEPARATOR) || "".equals(fqn))
            return root;

        if(n == null || child_name == null)
            return null;
        else
            return n.getChild(child_name);
    }


    void notifyNodeAdded(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeAdded(fqn);
    }

    void notifyNodeRemoved(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeRemoved(fqn);
    }

    void notifyNodeModified(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeModified(fqn);
    }

    void notifyViewChange(View v) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).viewChange(v);
    }

    /** Generates NodeAdded notifications for all nodes of the tree. This is called whenever the tree is
     initially retrieved (state transfer) */
    void notifyAllNodesCreated(Node curr) {
        Node n;
        Map children;

        if(curr == null) return;
        notifyNodeAdded(curr.fqn);
        if((children=curr.getChildren()) != null) {
            for(Iterator it=children.values().iterator(); it.hasNext();) {
                n=(Node)it.next();
                notifyAllNodesCreated(n);
            }
        }
    }


    public static class Node implements Serializable {
        private static final long serialVersionUID = -6687893970420949516L;
        String name=null;     // relative name (e.g. "Security")
        String fqn=null;      // fully qualified name (e.g. "/federations/fed1/servers/Security")
        Node parent=null;   // parent node
        TreeMap children=null; // keys: child name, value: Node
        HashMap data=null;     // data for current node
        // Address       creator=null;  // member that created this node (needed ?)


        protected Node(String child_name, String fqn, Node parent, HashMap data) {
            name=child_name;
            this.fqn=fqn;
            this.parent=parent;
            if(data != null) this.data=(HashMap)data.clone();
        }

        private Node(String child_name, String fqn, Node parent, String key, Object value) {
            name=child_name;
            this.fqn=fqn;
            this.parent=parent;
            if(data == null) data=new HashMap();
            data.put(key, value);
        }

        void setData(Map data) {
            if(data == null) return;
            if(this.data == null)
                this.data=new HashMap();
            this.data.putAll(data);
        }

        void setData(String key, Object value) {
            if(this.data == null)
                this.data=new HashMap();
            this.data.put(key, value);
        }

        HashMap getData() {
            return data;
        }

        Object getData(String key) {
            return data != null? data.get(key) : null;
        }


        boolean childExists(String child_name) {
            if(child_name == null) return false;
            return children != null? children.containsKey(child_name) : false;
        }


        Node createChild(String child_name, String fqn, Node parent, HashMap data) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=new TreeMap();
            child=(Node)children.get(child_name);
            if(child != null)
                child.setData(data);
            else {
                child=new Node(child_name, fqn, parent, data);
                children.put(child_name, child);
            }
            return child;
        }

        Node createChild(String child_name, String fqn, Node parent, String key, Object value) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=new TreeMap();
            child=(Node)children.get(child_name);
            if(child != null)
                child.setData(key, value);
            else {
                child=new Node(child_name, fqn, parent, key, value);
                children.put(child_name, child);
            }
            return child;
        }


        Node getChild(String child_name) {
            return child_name == null? null : children == null? null : (Node)children.get(child_name);
        }

        Map getChildren() {
            return children;
        }

        void removeData(String key) {
            if(data != null)
                data.remove(key);
        }

        void removeData() {
            if(data != null)
                data.clear();
        }

        void removeChild(String child_name, String fqn) {
            if(child_name != null && children != null && children.containsKey(child_name)) {
                children.remove(child_name);
            }
        }

        void removeAll() {
            if(children != null)
                children.clear();
        }

        void print(StringBuffer sb, int indent) {
            printIndent(sb, indent);
            sb.append(SEPARATOR).append(name);
            if(children != null && children.size() > 0) {
                Collection values=children.values();
                for(Iterator it=values.iterator(); it.hasNext();) {
                    sb.append('\n');
                    ((Node)it.next()).print(sb, indent + INDENT);
                }
            }
        }

        void printIndent(StringBuffer sb, int indent) {
            if(sb != null) {
                for(int i=0; i < indent; i++)
                    sb.append(' ');
            }
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            if(name != null) sb.append("\nname=" + name);
            if(fqn != null) sb.append("\nfqn=" + fqn);
            if(data != null) sb.append("\ndata=" + data);
            return sb.toString();
        }


        @Override // GemStoneAddition
        public Object clone() throws CloneNotSupportedException {
            Node n=new Node(name, fqn, parent != null? (Node)parent.clone() : null, data);
            if(children != null) n.children=(TreeMap)children.clone();
            return n;
        }

    }


    protected/*GemStoneAddition*/ static class StringHolder {
        String s=null;

        /*private GemStoneAddition*/ StringHolder() {
        }

        void setValue(String s) {
            this.s=s;
        }

        String getValue() {
            return s;
        }
    }


    /**
     * Class used to multicast add(), remove() and set() methods to all members.
     */
    private static class Request implements Serializable {
        private static final long serialVersionUID = 6700917610434765172L;
        static final int PUT=1;
        static final int REMOVE=2;

        int type=0;
        String fqn=null;
        String key=null;
        Object value=null;
        HashMap data=null;

        protected/*GemStoneAddition*/ Request(int type, String fqn) {
            this.type=type;
            this.fqn=fqn;
        }

        protected/*GemStoneAddition*/ Request(int type, String fqn, HashMap data) {
            this(type, fqn);
            this.data=data;
        }

        protected/*GemStoneAddition*/ Request(int type, String fqn, String key) {
            this(type, fqn);
            this.key=key;
        }

        protected/*GemStoneAddition*/ Request(int type, String fqn, String key, Object value) {
            this(type, fqn);
            this.key=key;
            this.value=value;
        }

        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(type2String(type)).append(" (");
            if(fqn != null) sb.append(" fqn=" + fqn);
            switch(type) {
                case PUT:
                    if(data != null) sb.append(", data=" + data);
                    if(key != null) sb.append(", key=" + key);
                    if(value != null) sb.append(", value=" + value);
                    break;
                case REMOVE:
                    if(key != null) sb.append(", key=" + key);
                    break;
                default:
                    break;
            }
            sb.append(')');
            return sb.toString();
        }

        static String type2String(int t) {
            switch(t) {
                case PUT:
                    return "PUT";
                case REMOVE:
                    return "REMOVE";
                default:
                    return "UNKNOWN";
            }
        }

    }


//    public static void main(String[] args) {
//
//
//        ReplicatedTree tree=null;
//        HashMap m=new HashMap();
//        String props;
//
//        props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
//                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
//                "PING(timeout=2000;num_initial_members=3):" +
//                "MERGE2(min_interval=5000;max_interval=10000):" +
//                "FD_SOCK:" +
//                "VERIFY_SUSPECT(timeout=1500):" +
//                "pbcast.STABLE(desired_avg_gossip=20000):" +
//                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
//                "UNICAST(timeout=5000):" +
//                "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
//                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
//                "shun=false;print_local_addr=true):" +
//                "pbcast.STATE_TRANSFER";
//        // "PERF(details=true)";
//
//        try {
//
//            tree=new ReplicatedTree(null, props, 10000);
//            // tree.setRemoteCalls(false);
//            tree.addReplicatedTreeListener(new MyListener());
//            tree.put("/a/b/c", null);
//            tree.put("/a/b/c1", null);
//            tree.put("/a/b/c2", null);
//            tree.put("/a/b1/chat", null);
//            tree.put("/a/b1/chat2", null);
//            tree.put("/a/b1/chat5", null);
//            System.out.println(tree);
//            m.put("name", "Bela Ban");
//            m.put("age", Integer.valueOf(36));
//            m.put("cube", "240-17");
//            tree.put("/a/b/c", m);
//            System.out.println("info for for \"/a/b/c\" is " + tree.print("/a/b/c"));
//            tree.put("/a/b/c", "age", Integer.valueOf(37));
//            System.out.println("info for for \"/a/b/c\" is " + tree.print("/a/b/c"));
//            tree.remove("/a/b");
//            System.out.println(tree);
//        }
//        catch(Exception ex) {
//            System.err.println(ex);
//        }
//    }


    static class MyListener implements ReplicatedTreeListener {

        public void nodeAdded(String fqn) {
            System.out.println("** node added: " + fqn);
        }

        public void nodeRemoved(String fqn) {
            System.out.println("** node removed: " + fqn);
        }

        public void nodeModified(String fqn) {
            System.out.println("** node modified: " + fqn);
        }

        public void viewChange(View new_view) {
            System.out.println("** view change: " + new_view);
        }

    }


}


