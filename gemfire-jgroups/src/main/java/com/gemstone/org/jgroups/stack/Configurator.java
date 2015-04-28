/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Configurator.java,v 1.14 2005/10/19 07:23:16 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.protocols.AUTH;
import com.gemstone.org.jgroups.protocols.FC;
import com.gemstone.org.jgroups.protocols.FD;
import com.gemstone.org.jgroups.protocols.FD_SOCK;
import com.gemstone.org.jgroups.protocols.FRAG2;
import com.gemstone.org.jgroups.protocols.FRAG3;
import com.gemstone.org.jgroups.protocols.TCPGOSSIP;
import com.gemstone.org.jgroups.protocols.UDP;
import com.gemstone.org.jgroups.protocols.UNICAST;
import com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT;
import com.gemstone.org.jgroups.protocols.VIEW_SYNC;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.protocols.pbcast.NAKACK;
import com.gemstone.org.jgroups.protocols.pbcast.STABLE;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.Util;

import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * The task if this class is to setup and configure the protocol stack. A string describing
 * the desired setup, which is both the layering and the configuration of each layer, is
 * given to the configurator which creates and configures the protocol stack and returns
 * a reference to the top layer (Protocol).<p>
 * Future functionality will include the capability to dynamically modify the layering
 * of the protocol stack and the properties of each layer.
 * @author Bela Ban
 */
public class Configurator {

     protected final GemFireTracer log=GemFireTracer.getLog(getClass());


    /**
     * The configuration string has a number of entries, separated by a ':' (colon).
     * Each entry consists of the name of the protocol, followed by an optional configuration
     * of that protocol. The configuration is enclosed in parentheses, and contains entries
     * which are name/value pairs connected with an assignment sign (=) and separated by
     * a semicolon.
     * <pre>UDP(in_port=5555;out_port=4445):FRAG(frag_size=1024)</pre><p>
     * The <em>first</em> entry defines the <em>bottommost</em> layer, the string is parsed
     * left to right and the protocol stack constructed bottom up. Example: the string
     * "UDP(in_port=5555):FRAG2(frag_size=32000):DEBUG" results is the following stack:<pre>
     *
     *   -----------------------
     *  | DEBUG                 |
     *  |-----------------------|
     *  | FRAG frag_size=32000  |
     *  |-----------------------|
     *  | UDP in_port=32000     |
     *   -----------------------
     * </pre>
     */
    public Protocol setupProtocolStack(String configuration, ProtocolStack st) throws Exception {
        Protocol protocol_stack=null;
        Vector protocol_configs;
        Vector protocols;

        protocol_configs=parseConfigurations(configuration);
        protocols=createProtocols(protocol_configs, st);
        if(protocols == null)
            return null;
        protocol_stack=connectProtocols(protocols);
        return protocol_stack;
    }


    public void startProtocolStack(Protocol bottom_prot) {
        while(bottom_prot != null) {
            bottom_prot.startDownHandler();
            bottom_prot.startUpHandler();
            bottom_prot=bottom_prot.getUpProtocol();
        }
    }


    public void stopProtocolStack(Protocol start_prot) {
        while(start_prot != null) {
            start_prot.stopInternal();
            start_prot.destroy();
            start_prot=start_prot.getDownProtocol();
        }
    }


    public Protocol findProtocol(Protocol prot_stack, String name) {
        String s;
        Protocol curr_prot=prot_stack;

        while(true) {
            s=curr_prot.getName();
            if(s == null)
                continue;
            if(s.equals(name))
                return curr_prot;
            curr_prot=curr_prot.getDownProtocol();
            if(curr_prot == null)
                break;
        }
        return null;
    }


    public Protocol getBottommostProtocol(Protocol prot_stack) {
        Protocol tmp=null, curr_prot=prot_stack;

        while(true) {
            if((tmp=curr_prot.getDownProtocol()) == null)
                break;
            curr_prot=tmp;
        }
        return curr_prot;
    }


    /**
     * Creates a new protocol given the protocol specification. Initializes the properties and starts the
     * up and down handler threads.
     * @param prot_spec The specification of the protocol. Same convention as for specifying a protocol stack.
     *                  An exception will be thrown if the class cannot be created. Example:
     *                  <pre>"VERIFY_SUSPECT(timeout=1500)"</pre> Note that no colons (:) have to be
     *                  specified
     * @param stack The protocol stack
     * @return Protocol The newly created protocol
     * @exception Exception Will be thrown when the new protocol cannot be created
     */
    public Protocol createProtocol(String prot_spec, ProtocolStack stack) throws Exception {
        ProtocolConfiguration config;
        Protocol prot;

        if(prot_spec == null) throw new Exception("Configurator.createProtocol(): prot_spec is null");

        // parse the configuration for this protocol
        config=new ProtocolConfiguration(prot_spec);

        // create an instance of the protocol class and configure it
        prot=config.createLayer(stack);

        // start the handler threads (unless down_thread or up_thread are set to false)
        prot.startDownHandler();
        prot.startUpHandler();

        return prot;
    }


    /**
     * Inserts an already created (and initialized) protocol into the protocol list. Sets the links
     * to the protocols above and below correctly and adjusts the linked list of protocols accordingly.
     * @param prot  The protocol to be inserted. Before insertion, a sanity check will ensure that none
     *              of the existing protocols have the same name as the new protocol.
     * @param position Where to place the protocol with respect to the neighbor_prot (ABOVE, BELOW)
     * @param neighbor_prot The name of the neighbor protocol. An exception will be thrown if this name
     *                      is not found
     * @param stack The protocol stack
     * @exception Exception Will be thrown when the new protocol cannot be created, or inserted.
     */
    public void insertProtocol(Protocol prot, int position, String neighbor_prot, ProtocolStack stack) throws Exception {
        if(neighbor_prot == null) throw new Exception("Configurator.insertProtocol(): neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new Exception("Configurator.insertProtocol(): position has to be ABOVE or BELOW");


        // find the neighbors below and above



        // connect to the protocol layer below and above


    }


    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public void removeProtocol(String prot_name) throws Exception {
    }



    /* ------------------------------- Private Methods ------------------------------------- */


    /**
     * Creates a protocol stack by iterating through the protocol list and connecting
     * adjacent layers. The list starts with the topmost layer and has the bottommost
     * layer at the tail. When all layers are connected the algorithms traverses the list
     * once more to call startInternal() on each layer.
     * @param protocol_list List of Protocol elements (from top to bottom)
     * @return Protocol stack
     */
    private Protocol connectProtocols(Vector protocol_list) {
        Protocol current_layer=null, next_layer=null;

        for(int i=0; i < protocol_list.size(); i++) {
            current_layer=(Protocol)protocol_list.elementAt(i);
            if(i + 1 >= protocol_list.size())
                break;
            next_layer=(Protocol)protocol_list.elementAt(i + 1);
            current_layer.setUpProtocol(next_layer);
            next_layer.setDownProtocol(current_layer);
        }
        return current_layer;
    }


    /**
     * Get a string of the form "P1(config_str1):P2:P3(config_str3)" and return
     * ProtocolConfigurations for it. That means, parse "P1(config_str1)", "P2" and
     * "P3(config_str3)"
     * @param config_str Configuration string
     * @return Vector of ProtocolConfigurations
     */
    public Vector parseComponentStrings(String config_str, String delimiter) {
        Vector retval=new Vector();
        StringTokenizer tok;
        String token;

        /*tok=new StringTokenizer(config_str, delimiter, false);
        while(tok.hasMoreTokens()) {
            token=tok.nextToken();
            retval.addElement(token);
        }*/
        // change suggested by gwoolsey
        tok=new StringTokenizer(config_str, delimiter, false);
        while(tok.hasMoreTokens()) {
            token=tok.nextToken().trim(); // GemStoneAddition - trim
            while(token.endsWith("\\"))
                token=token.substring(0, token.length() - 1) + delimiter + tok.nextToken();
            retval.addElement(token);
        }

        return retval;
    }


    /**
     * Return a number of ProtocolConfigurations in a vector
     * @param configuration protocol-stack configuration string
     * @return Vector of ProtocolConfigurations
     */
    public Vector parseConfigurations(String configuration) throws Exception {
        Vector retval=new Vector();
        // GemStoneAddition - use question mark, not colon to separate entries
        // so that numeric IPv6 addresses can be in the configuration
        Vector component_strings=parseComponentStrings(configuration, "?");
        String component_string;
        ProtocolConfiguration protocol_config;

        if(component_strings == null)
            return null;
        for(int i=0; i < component_strings.size(); i++) {
            component_string=(String)component_strings.elementAt(i);
            protocol_config=new ProtocolConfiguration(component_string);
            retval.addElement(protocol_config);
        }
        return retval;
    }


    /**
     * Takes vector of ProtocolConfigurations, iterates through it, creates Protocol for
     * each ProtocolConfiguration and returns all Protocols in a vector.
     * @param protocol_configs Vector of ProtocolConfigurations
     * @param stack The protocol stack
     * @return Vector of Protocols
     */
    private Vector createProtocols(Vector protocol_configs, ProtocolStack stack) throws Exception {
        Vector retval=new Vector();
        ProtocolConfiguration protocol_config;
        Protocol layer;

        for(int i=0; i < protocol_configs.size(); i++) {
            protocol_config=(ProtocolConfiguration)protocol_configs.elementAt(i);
            layer=protocol_config.createLayer(stack);
            if(layer == null)
                return null;
            retval.addElement(layer);
        }

        sanityCheck(retval);
        return retval;
    }


    /**
     Throws an exception if sanity check fails. Possible sanity check is uniqueness of all protocol
     names.
     */
    public void sanityCheck(Vector protocols) throws Exception {
        Vector names=new Vector();
        Protocol prot;
        String name;
        ProtocolReq req;
        Vector req_list=new Vector();
        int evt_type;

        // Checks for unique names
        for(int i=0; i < protocols.size(); i++) {
            prot=(Protocol)protocols.elementAt(i);
            name=prot.getName();
            for(int j=0; j < names.size(); j++) {
                if(name.equals(names.elementAt(j))) {
                    throw new Exception("Configurator.sanityCheck(): protocol name " + name +
                                        " has been used more than once; protocol names have to be unique !");
                }
            }
            names.addElement(name);
        }


        // Checks whether all requirements of all layers are met
        for(int i=0; i < protocols.size(); i++) {
            prot=(Protocol)protocols.elementAt(i);
            req=new ProtocolReq(prot.getName());
            req.up_reqs=prot.requiredUpServices();
            req.down_reqs=prot.requiredDownServices();
            req.up_provides=prot.providedUpServices();
            req.down_provides=prot.providedDownServices();
            req_list.addElement(req);
        }


        for(int i=0; i < req_list.size(); i++) {
            req=(ProtocolReq)req_list.elementAt(i);

            // check whether layers above this one provide corresponding down services
            if(req.up_reqs != null) {
                for(int j=0; j < req.up_reqs.size(); j++) {
                    evt_type=((Integer)req.up_reqs.elementAt(j)).intValue();

                    if(!providesDownServices(i, req_list, evt_type)) {
                        throw new Exception("Configurator.sanityCheck(): event " +
                                            Event.type2String(evt_type) + " is required by " +
                                            req.name + ", but not provided by any of the layers above");
                    }
                }
            }

            // check whether layers below this one provide corresponding up services
            if(req.down_reqs != null) {  // check whether layers above this one provide up_reqs
                for(int j=0; j < req.down_reqs.size(); j++) {
                    evt_type=((Integer)req.down_reqs.elementAt(j)).intValue();

                    if(!providesUpServices(i, req_list, evt_type)) {
                        throw new Exception("Configurator.sanityCheck(): event " +
                                            Event.type2String(evt_type) + " is required by " +
                                            req.name + ", but not provided by any of the layers below");
                    }
                }
            }

        }
    }


    /** Check whether any of the protocols 'below' end_index provide evt_type */
    boolean providesUpServices(int end_index, Vector req_list, int evt_type) {
        ProtocolReq req;

        for(int i=0; i < end_index; i++) {
            req=(ProtocolReq)req_list.elementAt(i);
            if(req.providesUpService(evt_type))
                return true;
        }
        return false;
    }


    /** Checks whether any of the protocols 'above' start_index provide evt_type */
    boolean providesDownServices(int start_index, Vector req_list, int evt_type) {
        ProtocolReq req;

        for(int i=start_index; i < req_list.size(); i++) {
            req=(ProtocolReq)req_list.elementAt(i);
            if(req.providesDownService(evt_type))
                return true;
        }
        return false;
    }



    /* --------------------------- End of Private Methods ---------------------------------- */





    private static class ProtocolReq {
        Vector up_reqs=null;
        Vector down_reqs=null;
        Vector up_provides=null;
        Vector down_provides=null;
        String name=null;

        ProtocolReq(String name) {
            this.name=name;
        }


        boolean providesUpService(int evt_type) {
            int type;

            if(up_provides != null) {
                for(int i=0; i < up_provides.size(); i++) {
                    type=((Integer)up_provides.elementAt(i)).intValue();
                    if(type == evt_type)
                        return true;
                }
            }
            return false;
        }

        boolean providesDownService(int evt_type) {
            int type;

            if(down_provides != null) {
                for(int i=0; i < down_provides.size(); i++) {
                    type=((Integer)down_provides.elementAt(i)).intValue();
                    if(type == evt_type)
                        return true;
                }
            }
            return false;
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer ret=new StringBuffer();
            ret.append('\n' + name + ':');
            if(up_reqs != null)
                ret.append("\nRequires from above: " + printUpReqs());

            if(down_reqs != null)
                ret.append("\nRequires from below: " + printDownReqs());

            if(up_provides != null)
                ret.append("\nProvides to above: " + printUpProvides());

            if(down_provides != null)
                ret.append("\nProvides to below: ").append(printDownProvides());
            return ret.toString();
        }


        String printUpReqs() {
            StringBuffer ret=new StringBuffer("[");
            if(up_reqs != null) {
                for(int i=0; i < up_reqs.size(); i++) {
                    ret.append(Event.type2String(((Integer)up_reqs.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }

        String printDownReqs() {
            StringBuffer ret=new StringBuffer("[");
            if(down_reqs != null) {
                for(int i=0; i < down_reqs.size(); i++) {
                    ret.append(Event.type2String(((Integer)down_reqs.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }


        String printUpProvides() {
            StringBuffer ret=new StringBuffer("[");
            if(up_provides != null) {
                for(int i=0; i < up_provides.size(); i++) {
                    ret.append(Event.type2String(((Integer)up_provides.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }

        String printDownProvides() {
            StringBuffer ret=new StringBuffer("[");
            if(down_provides != null) {
                for(int i=0; i < down_provides.size(); i++)
                    ret.append(Event.type2String(((Integer)down_provides.elementAt(i)).intValue()) +
                               ' ');
            }
            return ret.toString() + ']';
        }

    }


    /**
     * Parses and encapsulates the specification for 1 protocol of the protocol stack, e.g.
     * <code>UNICAST(timeout=5000)</code>
     */
    public class ProtocolConfiguration {
        private String protocol_name=null;
        private String properties_str=null;
        private final Properties properties=new Properties();
        private static final String protocol_prefix="org.jgroups.protocols";


        /**
         * Creates a new ProtocolConfiguration.
         * @param config_str The configuration specification for the protocol, e.g.
         *                   <pre>VERIFY_SUSPECT(timeout=1500)</pre>
         */
        public ProtocolConfiguration(String config_str) throws Exception {
            setContents(config_str);
        }

        public String getProtocolName() {
            return protocol_name;
        }

        public Properties getProperties() {
            return properties;
        }


        void setContents(String config_str) throws Exception {
            int index=config_str.indexOf('(');  // e.g. "UDP(in_port=3333)"
            int end_index=config_str.lastIndexOf(')');

            if(index == -1) {
                protocol_name=config_str;
            }
            else {
                if(end_index == -1) {
                    throw new Exception("Configurator.ProtocolConfiguration.setContents(): closing ')' " +
                                        "not found in " + config_str + ": properties cannot be set !");
                }
                else {
                    properties_str=config_str.substring(index + 1, end_index);
                    protocol_name=config_str.substring(0, index);
                }
            }

            /* "in_port=5555;out_port=6666" */
            if(properties_str != null) {
                Vector components=parseComponentStrings(properties_str, ";");
                if(components.size() > 0) {
                    for(int i=0; i < components.size(); i++) {
                        String name, value, comp=(String)components.elementAt(i);
                        index=comp.indexOf('=');
                        if(index == -1) {
                            throw new Exception("Configurator.ProtocolConfiguration.setContents(): " +
                                                "'=' not found in " + comp);
                        }
                        name=comp.substring(0, index);
                        value=comp.substring(index + 1, comp.length());
                        properties.put(name, value);
                    }
                }
            }
        }


      // GemStoneAddition - to speed up stack instantiation for our stack
      private static final String UDP =
        "com.gemstone.org.jgroups.protocols.UDP";
      private static final String TCPGOSSIP =
        "com.gemstone.org.jgroups.protocols.TCPGOSSIP"; 
      private static final String FD_SOCK =
        "com.gemstone.org.jgroups.protocols.FD_SOCK";
      private static final String FD =
        "com.gemstone.org.jgroups.protocols.FD";
      private static final String VERIFY_SUSPECT =
        "com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT";
      private static final String STABLE =
        "com.gemstone.org.jgroups.protocols.pbcast.STABLE";
      private static final String NAKACK =
        "com.gemstone.org.jgroups.protocols.pbcast.NAKACK";
      private static final String UNICAST =
        "com.gemstone.org.jgroups.protocols.UNICAST";
      private static final String FRAG2 =
        "com.gemstone.org.jgroups.protocols.FRAG2";
      private static final String FRAG3 = // GemStoneAddition - new fragmentation protocol
        "com.gemstone.org.jgroups.protocols.FRAG3";
      private static final String AUTH =
        "com.gemstone.org.jgroups.protocols.AUTH";
      private static final String GMS =
        "com.gemstone.org.jgroups.protocols.pbcast.GMS";
      private static final String VIEW_SYNC =
        "com.gemstone.org.jgroups.protocols.VIEW_SYNC";
//      private static final String STATE_TRANSFER = GemStoneAddition (omitted)
//        "com.gemstone.org.jgroups.protocols.pbcast.STATE_TRANSFER";
      private static final String FC =
        "com.gemstone.org.jgroups.protocols.FC";
      private static final String GemFireTimeSync = "com.gemstone.org.jgroups.protocols.GemFireTimeSync";

        Protocol createLayer(ProtocolStack prot_stack) throws Exception {
            Protocol retval=null;
            if(protocol_name == null)
                return null;

            // [GemStoneAddition] Handle the protocol we use specially to
            // avoid all of that reflection.
            if (protocol_name.equals(UDP)) {
              retval = new UDP();

            } else if (protocol_name.equals(TCPGOSSIP)) {
              retval = new TCPGOSSIP();

            } else if (protocol_name.equals(FD_SOCK)) {
              retval = new FD_SOCK();

            } else if (protocol_name.equals(FD)) {
              retval = new FD();

            } else if (protocol_name.equals(VERIFY_SUSPECT)) {
              retval = new VERIFY_SUSPECT();

            } else if (protocol_name.equals(STABLE)) {
              retval = new STABLE();

            } else if (protocol_name.equals(NAKACK)) {
              retval = new NAKACK();

            } else if (protocol_name.equals(UNICAST)) {
              retval = new UNICAST();

            } else if (protocol_name.equals(FRAG2)) {
              retval = new FRAG2();
              
            } else if (protocol_name.equals(FRAG3)) {
              retval = new FRAG3();

            } else if (protocol_name.equals(AUTH)) {
              retval = new AUTH();

            } else if (protocol_name.equals(GMS)) {
              retval = new GMS();

            } else if (protocol_name.equals(VIEW_SYNC)) {
              retval = new VIEW_SYNC();

            } else if (protocol_name.equals(FC)) {
              retval = new FC();

            } else if (protocol_name.equals(GemFireTimeSync)) {
              retval = new com.gemstone.org.jgroups.protocols.GemFireTimeSync();

            }

            
            
          if (retval == null) { // GemStoneAddition
            String defaultProtocolName=protocol_prefix + '.' + protocol_name;
            Class clazz=null;

            try {
                clazz=Util.loadClass(defaultProtocolName, this.getClass());
            }
            catch(ClassNotFoundException e) {
            }

            if(clazz == null) {
                try {
                    clazz=Util.loadClass(protocol_name, this.getClass());
                }
                catch(ClassNotFoundException e) {
                }
                if(clazz == null) {
                    throw new Exception("unable to load class for protocol " + protocol_name +
                            " (either as an absolute - " + protocol_name + " - or relative - " +
                            defaultProtocolName + " - package name)!");
                }
            }

            try {
                retval=(Protocol)clazz.newInstance();

                if(retval == null)
                    throw new Exception("creation of instance for protocol " + protocol_name + "failed !");
                retval.setProtocolStack(prot_stack);
                if(properties != null)
                    if(!retval.setPropertiesInternal(properties))
                        return null;
                retval.init();
            }
            catch(InstantiationException inst_ex) {
                log.error("an instance of " + protocol_name + " could not be created. Please check that it implements" +
                        " interface Protocol and that is has a public empty constructor !");
                throw inst_ex;
            }
          } //endif GemStoneAddition

          else { // GemStoneAddition - copied from above try/catch block to initialize the protocol
                retval.setProtocolStack(prot_stack);
                if(properties != null)
                    if(!retval.setPropertiesInternal(properties))
                        return null;
                retval.init();
          }
            return retval;
        }


        @Override // GemStoneAddition
        public String toString() {
            StringBuffer retval=new StringBuffer();
            retval.append("Protocol: ");
            if(protocol_name == null)
                retval.append("<unknown>");
            else
                retval.append(protocol_name);
            if(properties != null)
                retval.append("(" + properties + ')');
            return retval.toString();
        }
    }


//    public static void main(String args[]) {
//        if(args.length != 1) {
//            System.err.println("Configurator <string>");
//            System.exit(0);
//        }
//        String config_str=args[0];
//        Configurator conf=new Configurator();
//        Vector protocol_configs;
//        Vector protocols=null;
//        Protocol protocol_stack;
//
//
//        try {
//            protocol_configs=conf.parseConfigurations(config_str);
//            protocols=conf.createProtocols(protocol_configs, null);
//            if(protocols == null)
//                return;
//            protocol_stack=conf.connectProtocols(protocols);
//            Thread.sleep(3000);
//            conf.stopProtocolStack(protocol_stack);
//            // conf.stopProtocolStackInternal(protocol_stack);
//        }
//        catch(Exception e) {
//            System.err.println(e);
//        }
//
//        System.err.println(protocols);
//    }


}


