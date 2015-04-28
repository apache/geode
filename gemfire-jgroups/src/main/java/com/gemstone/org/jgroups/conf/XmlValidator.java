/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: XmlValidator.java,v 1.5 2004/09/23 16:29:15 belaban Exp $

package com.gemstone.org.jgroups.conf;

/**
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
//Add these lines to import the JAXP APIs you'll be using:
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.gemstone.org.jgroups.util.PrintXMLTree;
import com.gemstone.org.jgroups.util.Util;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URL;
public class XmlValidator
{

    private final String mXmlUrl;
    private final String mDtdUrl;
    private final boolean mPrintXml;
    
    public XmlValidator(String xmlUrl, String dtdUrl)
    {
        this(xmlUrl,dtdUrl,true);
    }
    
    public XmlValidator(String xmlUrl, String dtdUrl, boolean printXml)
    {
        mXmlUrl = xmlUrl;
        mDtdUrl = dtdUrl;
        mPrintXml = printXml;
    }
    
    protected static InputStream getInputStream(String url)
        throws java.io.IOException
    {
        URL inurl = new URL(url);
        return inurl.openStream();
    }
    
    
    public void validate() throws Exception
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true); //for now
        
        DocumentBuilder builder = factory.newDocumentBuilder();
        //Document document = builder.parse( this.getInputStream( mXmlUrl ),mDtdUrl );
        builder.setEntityResolver(new JGEntityResolver(mDtdUrl));
        JGErrorHandler errorhandler = new JGErrorHandler();
        builder.setErrorHandler(errorhandler);
        Document document = builder.parse( getInputStream( mXmlUrl ));
        if ( mPrintXml )
            PrintXMLTree.print(new java.io.PrintWriter(System.out),document.getDocumentElement());
        System.out.println("\n\nError Report:");
        System.out.println(errorhandler.getErrorString());
        
    }
    
    public static void main(String[] args)
        throws Exception
    {
    
        if ( args.length < 2 )
        {
            System.out.println("Usage: XmlValidator xmlUrl dtdUrl [printXml(true|false)]");
            System.out.println("Example: XmlValidator file:/usr/local/test.xml file:conf/jgroups-protocol.dtd false");
            System.exit(1);
        }
        boolean printXml = true;
        if ( args.length > 2 )
        {
            try
            {
                printXml = Boolean.valueOf(args[2]).booleanValue();
            }
            catch ( Exception x){ x.printStackTrace();}
        }
        XmlValidator xmlValidator = new XmlValidator(args[0],args[1],printXml);
        xmlValidator.validate();
    }
    
    static/*GemStoneAddition*/ class JGEntityResolver implements org.xml.sax.EntityResolver 
    {
        private final String mDtdUrl;
        public JGEntityResolver(String dtdUrl)
        {
            mDtdUrl = dtdUrl;
        }
        public InputSource resolveEntity(java.lang.String publicId,
                                         java.lang.String systemId)
                          throws SAXException,
                                 java.io.IOException
        {
            
            InputSource source = new InputSource(getInputStream(mDtdUrl));
            return source;
        }
    }
    
    static/*GemStoneAddition*/ class JGErrorHandler implements org.xml.sax.ErrorHandler
    {
        private final StringBuffer mErrors = new StringBuffer();
        int count = 1;
        public void warning(SAXParseException exception)
             throws SAXException
        {
            mErrors.append('\n').append(count++).append(". WARNING: ");
            mErrors.append(Util.getStackTrace(exception));
            mErrors.append('\n');
        }
        
        public void error(SAXParseException exception)
             throws SAXException
        {
            mErrors.append('\n').append(count++).append(". ERROR: ");
            mErrors.append(Util.getStackTrace(exception));
            mErrors.append('\n');
        }
        
        public void fatalError(SAXParseException exception)
             throws SAXException
        {
            mErrors.append('\n').append(count++).append(". FATAL ERROR: ");
            mErrors.append(Util.getStackTrace(exception));
            mErrors.append('\n');
        }
        
        public String getErrorString()
        {
            if ( mErrors.length() == 0 ) return "No errors reported."; else return mErrors.toString();
        }
    
    }
}
