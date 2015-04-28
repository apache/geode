/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: XmlConfigurator.java,v 1.15 2005/11/03 11:42:58 belaban Exp $

package com.gemstone.org.jgroups.conf;

/**
 * Uses XML to configure a protocol stack
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;


public class XmlConfigurator implements ProtocolStackConfigurator {
    public static final String ATTR_NAME="name";
    public static final String ATTR_VALUE="value";
    public static final String ATTR_INHERIT="inherit";
    public static final String ELMT_PROT_OVERRIDE="protocol-override";
    public static final String ELMT_PROT="protocol";
    public static final String ELMT_PROT_NAME="protocol-name";
    public static final String ELMT_CLASS="class-name";
    public static final String ELMT_DESCRIPTION="description";
    public static final String ELMT_PROT_PARAMS="protocol-params";

    private final ArrayList mProtocolStack=new ArrayList();
    private final String mStackName;
    protected static final GemFireTracer log=GemFireTracer.getLog(XmlConfigurator.class);

    protected XmlConfigurator(String stackName, ProtocolData[] protocols) {
        mStackName=stackName;
        for(int i=0; i < protocols.length; i++)
            mProtocolStack.add(protocols[i]);
    }

    protected XmlConfigurator(String stackName) {
        this(stackName, new ProtocolData[0]);
    }


    public static XmlConfigurator getInstance(URL url) throws java.io.IOException {
        return getInstance(url.openStream());
    }

    public static XmlConfigurator getInstanceOldFormat(URL url) throws java.io.IOException {
        return getInstanceOldFormat(url.openStream());
    }

    public static XmlConfigurator getInstance(InputStream stream) throws java.io.IOException {
        return parse(stream);
    }

    public static XmlConfigurator getInstanceOldFormat(InputStream stream) throws java.io.IOException {
        return parseOldFormat(stream);
    }


    public static XmlConfigurator getInstance(Element el) throws java.io.IOException {
        return parse(el);
    }


    /**
     *
     * @param convert If false: print old plain output, else print new XML format
     * @return String with protocol stack in specified format
     */
    public String getProtocolStackString(boolean convert) {
        StringBuffer buf=new StringBuffer();
        Iterator it=mProtocolStack.iterator();
        if(convert) buf.append("<config>\n");
        while(it.hasNext()) {
            ProtocolData d=(ProtocolData)it.next();
            if(convert) buf.append("    <");
            buf.append(d.getProtocolString(convert));
            if(convert) buf.append("/>");
            if(it.hasNext()) {
                if(convert)
                    buf.append('\n');
                else
                    buf.append(':');
            }
        }
        if(convert) buf.append("\n</config>");
        return buf.toString();
    }


    public String getProtocolStackString() {
        return getProtocolStackString(false);
    }


    public ProtocolData[] getProtocolStack() {
        return (ProtocolData[])mProtocolStack.toArray(new ProtocolData[mProtocolStack.size()]);
    }

    public String getName() {
        return mStackName;
    }

    public void override(ProtocolData data)
            throws IOException {
        int index=mProtocolStack.indexOf(data);
        if(index < 0) throw new IOException("You can not override a protocol that doesn't exist");
        ProtocolData source=(ProtocolData)mProtocolStack.get(index);
        source.override(data.getParametersAsArray());
    }

    public void add(ProtocolData data) {
        mProtocolStack.add(data);
    }



    protected static XmlConfigurator parseOldFormat(InputStream stream) throws java.io.IOException {
        XmlConfigurator configurator=null;
        try {
            DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
            factory.setValidating(false); //for now
            DocumentBuilder builder=factory.newDocumentBuilder();
            builder.setEntityResolver(new ClassPathEntityResolver());
            Document document=builder.parse(stream);
            Element root=(Element)document.getElementsByTagName("protocol-stack").item(0);
            root.normalize();
            //print("",new PrintWriter(System.out),root);
            String stackname=root.getAttribute(ATTR_NAME);
            String inherit=root.getAttribute(ATTR_INHERIT);
            boolean isinherited=(inherit != null && inherit.length() > 0);
            NodeList protocol_list=document.getElementsByTagName(isinherited ? ELMT_PROT_OVERRIDE : ELMT_PROT);
            Vector v=new Vector();
            for(int i=0; i < protocol_list.getLength(); i++) {
                if(protocol_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    v.addElement(parseProtocolData(protocol_list.item(i)));
                }
            }
            ProtocolData[] protocols=new ProtocolData[v.size()];
            v.copyInto(protocols);

            if(isinherited) {
                URL inheritURL=new URL(inherit);
                configurator=XmlConfigurator.getInstance(inheritURL);
                for(int i=0; i < protocols.length; i++)
                    configurator.override(protocols[i]);
            }
            else {
                configurator=new XmlConfigurator(stackname, protocols);
            }

        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
        return configurator;
    }




    protected static XmlConfigurator parse(InputStream stream) throws java.io.IOException {
        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
         * But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
         * If somebody wants to improve this, please be my guest.
         */
        try {
            DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
            factory.setValidating(false); //for now
            DocumentBuilder builder=factory.newDocumentBuilder();
            builder.setEntityResolver(new ClassPathEntityResolver());
            Document document=builder.parse(stream);

            // The root element of the document should be the "config" element,
            // but the parser(Element) method checks this so a check is not
            // needed here.
            Element configElement = document.getDocumentElement();
            return parse(configElement);
        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
    }




    protected static XmlConfigurator parse(Element root_element) throws java.io.IOException {
        XmlConfigurator configurator=null;

        /** LinkedList<ProtocolData> */
        LinkedList prot_data=new LinkedList();


        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
         * But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
         * If somebody wants to improve this, please be my guest.
         */
        try {
            Node root=root_element;
//            NodeList roots=root_element.getChildNodes();
//            for(int i =0; i < roots.getLength(); i++) {
//                root=roots.item(i);
//                if(root.getNodeType() != Node.ELEMENT_NODE)
//                    continue;
//            }

            String root_name=root.getNodeName();
            if(!"config".equals(root_name.trim().toLowerCase())) {
                log.fatal("XML protocol stack configuration does not start with a '<config>' element; " +
                        "maybe the XML configuration needs to be converted to the new format ?\n" +
                        "use 'java org.jgroups.conf.XmlConfigurator <old XML file> -new_format' to do so");
                throw new IOException("invalid XML configuration");
            }

            NodeList prots=root.getChildNodes();
            for(int i=0; i < prots.getLength(); i++) {
                Node node = prots.item(i);
                if( node.getNodeType() != Node.ELEMENT_NODE )
                    continue;

                Element tag = (Element) node;
                String protocol = tag.getTagName();
                // System.out.println("protocol: " + protocol);
                LinkedList tmp=new LinkedList();

                NamedNodeMap attrs = tag.getAttributes();
                int attrLength = attrs.getLength();
                for(int a = 0; a < attrLength; a ++) {
                    Attr attr = (Attr) attrs.item(a);
                    String name = attr.getName();
                    String value = attr.getValue();
                    // System.out.println("    name=" + name + ", value=" + value);
                    tmp.add(new ProtocolParameter(name, value));
                }
                ProtocolParameter[] params=new ProtocolParameter[tmp.size()];
                for(int j=0; j < tmp.size(); j++)
                    params[j]=(ProtocolParameter)tmp.get(j);
                ProtocolData data=new ProtocolData(protocol, "bla", "" + protocol, params);
                prot_data.add(data);
            }

            ProtocolData[] data=new ProtocolData[(prot_data.size())];
            for(int k=0; k < prot_data.size(); k++)
                data[k]=(ProtocolData)prot_data.get(k);
            configurator=new XmlConfigurator("bla", data);
        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
        return configurator;
    }


    protected static ProtocolData parseProtocolData(Node protocol)
            throws java.io.IOException {
        try {
            protocol.normalize();
            boolean isOverride=ELMT_PROT_OVERRIDE.equals(protocol.getNodeName());
            int pos=0;
            NodeList children=protocol.getChildNodes();
            /**
             * there should be 4 Element Nodes if we are not overriding
             * 1. protocol-name
             * 2. description
             * 3. class-name
             * 4. protocol-params
             *
             * If we are overriding we should have
             * 1. protocol-name
             * 2. protocol-params
             */

            //

            String name=null;
            String clazzname=null;
            String desc=null;
            ProtocolParameter[] plist=null;

            for(int i=0; i < children.getLength(); i++) {
                if(children.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    pos++;
                    if(isOverride && (pos == 2)) pos=4;
                    switch(pos) {
                        case 1:
                            name=children.item(i).getFirstChild().getNodeValue();
                            break;
                        case 2:
                            desc=children.item(i).getFirstChild().getNodeValue();
                            break;
                        case 3:
                            clazzname=children.item(i).getFirstChild().getNodeValue();
                            break;
                        case 4:
                            plist=parseProtocolParameters((Element)children.item(i));
                            break;
                    }//switch
                }//end if
            }//for

            if(isOverride)
                return new ProtocolData(name, plist);
            else
                return new ProtocolData(name, desc, clazzname, plist);
        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
    }

    protected static ProtocolParameter[] parseProtocolParameters(Element protparams)
            throws IOException {

        try {
            Vector v=new Vector();
            protparams.normalize();
            NodeList parameters=protparams.getChildNodes();
            for(int i=0; i < parameters.getLength(); i++) {
                if(parameters.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    String pname=parameters.item(i).getAttributes().getNamedItem(ATTR_NAME).getNodeValue();
                    String pvalue=parameters.item(i).getAttributes().getNamedItem(ATTR_VALUE).getNodeValue();
                    ProtocolParameter p=new ProtocolParameter(pname, pvalue);
                    v.addElement(p);
                }//end if
            }//for
            ProtocolParameter[] result=new ProtocolParameter[v.size()];
            v.copyInto(result);
            return result;
        }
        catch(Exception x) {
            if(x instanceof java.io.IOException)
                throw (java.io.IOException)x;
            else {
                IOException tmp=new IOException();
                tmp.initCause(x);
                throw tmp;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String input_file=null, output=null;
        XmlConfigurator conf;
        boolean new_format=false;

        if(args.length == 0) {
            help();
            return;
        }

        input_file=args[0];

        for(int i=1; i < args.length; i++) {
            if("-new_format".equals(args[i])) {
                new_format=true;
                continue;
            }
            help();
            return;
        }
        if(input_file != null) {
            InputStream input=null;

            try {
                input=new FileInputStream(new File(input_file));
            }
            catch(Throwable t) {
            }
            if(input == null) {
                try {
                    input=new URL(input_file).openStream();
                }
                catch(Throwable t) {
                }
            }

            conf=XmlConfigurator.getInstanceOldFormat(input);
            output=conf.getProtocolStackString(new_format);
            output=replace(output, "org.jgroups.protocols.", "");
            if(new_format)
                System.out.println(getTitle(input_file));
            System.out.println('\n' + output);
        }
        else {
            log.error(ExternalStrings.XmlConfigurator_NO_INPUT_FILE_GIVEN);
        }
    }

    private static String getTitle(String input) {
        StringBuffer sb=new StringBuffer();
        sb.append("\n\n<!-- ************ JGroups Protocol Stack Configuration ************** -->\n");
        sb.append("<!-- generated by XmlConfigurator on " + new Date() + " -->\n");
        sb.append("<!-- input file: " + input + " -->");
        return sb.toString();
    }


    public static String replace(String input, final String expr, String replacement) {
        StringBuffer sb=new StringBuffer();
        int new_index=0, index=0, len=expr.length(), input_len=input.length();

        while(true) {
            new_index=input.indexOf(expr, index);
            if(new_index == -1) {
                sb.append(input.substring(index, input_len));
                break;
            }
            sb.append(input.substring(index, new_index));
            sb.append(replacement);
            index=new_index + len;
        }


        return sb.toString();
    }


    static void help() {
        System.out.println("XmlConfigurator <input XML file> [-new_format]");
        System.out.println("(-new_format: converts old XML format into new format)");
    }
}
