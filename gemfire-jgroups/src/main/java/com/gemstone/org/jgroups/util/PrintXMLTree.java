/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PrintXMLTree.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package com.gemstone.org.jgroups.util;


/**
 * Title:        JGroups Communications
 * Description:  Contact me at <a href="mailto:filip@filip.net">filip@filip.net</a>
 * Copyright:    Copyright (c) 2002
 * Company:      www.filip.net
 * @author Filip Hanik
 * @version 1.0
 */
//Finally, import the W3C definition for a DOM and DOM exceptions:

import org.w3c.dom.*;

import java.io.PrintWriter;




public class PrintXMLTree {

    public PrintXMLTree() {
    }


    public static void print(PrintWriter out,
                             Element root) {

        print("", out, root);
    }

    protected static void print(String prepend,
                                PrintWriter out,
                                Element root) {

        // get elements that match
//        String attributeName=null;
        if(root == null) return;

        print(prepend, out, root, root.getAttributes());

        NodeList elements=root.getChildNodes();

        // is there anything to do?
        if(elements == null) return;

        // print all elements
//        if(attributeName == null) (can only be null) 
        {
            int elementCount=elements.getLength();
            for(int i=0; i < elementCount; i++) {
                if(elements.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    Element element=(Element)elements.item(i);
                    print(prepend, out, element, element.getAttributes());
                }
            }
        }
//        // print elements with given attribute name
//        else {
//            int elementCount=elements.getLength();
//            for(int i=0; i < elementCount; i++) {
//                Element element=(Element)elements.item(i);
//                NamedNodeMap attributes=element.getAttributes();
//                if(attributes.getNamedItem(attributeName) != null) {
//                    print(prepend, out, element, attributes);
//                }
//            }
//        }

    } // print(PrintWriter,Document,String,String)


    /** Prints the specified element. */
    protected static void print(String prepend,
                                PrintWriter out,
                                Element element,
                                NamedNodeMap attributes) {

        out.print(prepend);
        out.print('<');
        out.print(element.getNodeName());

        if(attributes != null) {
            int attributeCount=attributes.getLength();
            for(int i=0; i < attributeCount; i++) {
                Attr attribute=(Attr)attributes.item(i);
                out.print(' ');
                out.print(attribute.getNodeName());
                out.print("=\"");
                out.print(normalize(attribute.getNodeValue()));
                out.print('"');
            }
        }
        out.println('>');


        NodeList list=element.getChildNodes();

        for(int i=0; i < list.getLength(); i++) {
            if(list.item(i).getNodeType() == Node.ELEMENT_NODE)
                print(prepend + "  ", out, (Element)list.item(i));
            else
                if(list.item(i).getNodeType() == Node.TEXT_NODE &&
                        list.item(i).getNodeValue().trim().length() > 0)
                    out.println(prepend + "  " + list.item(i).getNodeValue());
        }

        out.print(prepend + "</");
        out.print(element.getNodeName());
        out.println(">");

        out.flush();

    } // print(PrintWriter,Element,NamedNodeMap)

    public static String normalize(String s) {
        StringBuffer str=new StringBuffer();

        int len=(s != null)? s.length() : 0;
        for(int i=0; i < len; i++) {
            char ch=s.charAt(i);
            switch(ch) {
                case '<':
                    {
                        str.append("&lt;");
                        break;
                    }
                case '>':
                    {
                        str.append("&gt;");
                        break;
                    }
                case '&':
                    {
                        str.append("&amp;");
                        break;
                    }
                case '"':
                    {
                        str.append("&quot;");
                        break;
                    }
                case '\r':
                case '\n':
                    {
                        str.append("&#");
                        str.append(Integer.toString(ch));
                        str.append(';');
                        break;
                    }
                default:
                    {
                        str.append(ch);
                    }
            }
        }

        return str.toString();

    } // normalize(String):String
}
