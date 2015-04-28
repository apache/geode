/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/


package com.gemstone.org.jgroups.conf;

/**
 * Reads and maintains mapping between magic numbers and classes
 * <br>[GemStoneAddition] this is actually a heavily modified reader from
 * 2.0.3.  It uses SAX instead of DOM and has options for reading from a
 * text file instead of an expensive xml file<br>
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
//import java.net.URL;
import java.io.*;


//Add these lines to import the JAXP APIs you'll be using:
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;


//Add these lines for the exceptions that can be thrown when the XML document is parsed: 
import org.xml.sax.Attributes;  
import org.xml.sax.EntityResolver;  
import org.xml.sax.InputSource;  
import org.xml.sax.SAXException;  
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;  


//Add these lines to read the sample XML file and identify errors: 
//import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.util.*;

/**
 * Reads the file containing the map of magic numbers for Jgroups
 *
 * <P>
 *
 * [GemStone] This class originally used DOM to parse the XML data.
 * This was very slow, so it was rewritten to use SAX parsing.
 */
public class MagicNumberReader
  {
    private static boolean xml_debug = false;
    public static final String MAGIC_NUMBER_FILE = "jg-magic-map.xml";
    public static final String MAGIC_NUMBER_TEXT_FILE = "jg-magic-map.txt";
    
    public String mMagicNumberFile = MAGIC_NUMBER_FILE;
    
  private static GemFireTracer getLog() {
    return GemFireTracer.getLog(GemFireTracer.class);
  }
  
  /**
   * [GemStone] Finds and parses the TEXT version of the magic number
   * config file
   */
  public static ClassMap[] readMagicNumberMappingFromText() {
    ClassMap[] clsMapRet = new ClassMap[0];
    InputStream stream = null;
    try {
      // [GemStoneAddition] load resource relative to class, not loader
      stream = MagicNumberReader.class.getResourceAsStream(MAGIC_NUMBER_TEXT_FILE);
      if(stream == null) {
        if (getLog().isWarnEnabled())
          getLog().warn("MagicNumberReader.readMagicNumberMappingFromText(): failed reading " +
                   MAGIC_NUMBER_TEXT_FILE + ". Please make sure it is in the CLASSPATH. Will " +
                   "continue, but marshalling will be slower");
      }
      else {
        clsMapRet = parseText(stream);
      }
    } catch ( Exception x ) {
      if ( xml_debug ) x.printStackTrace();
      getLog().error(ExternalStrings.MagicNumberReader_MAGICNUMBERREADERREADMAGICNUMBERMAPPING, x);
    }
    finally {
      if( null != stream ) {
        try {
          stream.close();
        } catch( IOException x ) {
          getLog().warn("MagicNumberReader.readMagicNumberMappingFromText(): exception on closing InputStream ", x);
        }
      }
//      getLog().warn("read " + MAGIC_NUMBER_TEXT_FILE);
    }

    return clsMapRet; 
  }

  /**
   * [GemStone] Parses the stream as a text version of the magic
   * number file.
   */
  protected static ClassMap[] parseText(InputStream stream) 
    throws IOException {

    List classMaps = new ArrayList();

      BufferedReader br =
        new BufferedReader(new InputStreamReader(stream, "US-ASCII"));
      while (br.ready()) {
        String line = br.readLine();
        if (line == null) break; // GemStoneAddition
//getLog().info("read line '" + line + "'");
        StringTokenizer st = new StringTokenizer(line, "/");
        if (!st.hasMoreTokens()) {
          continue;
        }
        
        String className = st.nextToken();
//getLog().info("read classname " + className);
        String description = st.nextToken();
        boolean preload =
          Boolean.valueOf(st.nextToken()).booleanValue();
        int magicNumber = Integer.parseInt(st.nextToken());
        classMaps.add(new ClassMap(className, description, preload,
                                   magicNumber));
      }

    return (ClassMap[]) classMaps.toArray(new ClassMap[0]);
  }

    public void setFilename(String file)
    {
        mMagicNumberFile = file;
    }
    
    public ClassMap[] readMagicNumberMapping()
    {
        try
        {
          // [GemStone] load resource relative to class, not loader
            InputStream stream = getClass().getClassLoader().getResourceAsStream(mMagicNumberFile);
	    if(stream == null) {
		getLog().warn("MagicNumberReader.readMagicNumberMapping(): failed reading " +
			   mMagicNumberFile + ". Please make sure it is in the CLASSPATH. Will " +
			   "continue, but marshalling will be slower");
		return new ClassMap[0];
	    }
            return parse(stream);
        }
        catch(Exception x) {
            if(xml_debug) x.printStackTrace();
            getLog().error(ExternalStrings.MagicNumberReader_MAGICNUMBERREADERREADMAGICNUMBERMAPPING, x);
        }
        return new ClassMap[0];
    }
    
  /**
   * [GemStone] Rewritten to use SAX parsing instead of DOM.
   * Note that stdout will be going to the magic number file, so
   * don't use logging or System.out here.
   */
  protected static ClassMap[] parse(InputStream stream) throws Exception {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(false); // for now
    SAXParser parser = factory.newSAXParser();
    XmlHandler handler = new XmlHandler();
    parser.parse(stream, handler);
    return handler.getClassMaps();

  }//parse
    
  /**
   * [GemStone] Main method that parses a file containing magic
   * numbers and prints the results.
   */
  public static void main(String[] args) throws Throwable {
    ClassMap[] classes = null;
    
    if (args.length < 1) {
      //System.err.println("\n** Missing file name\n");
      //System.exit(1);
      classes = readMagicNumberMappingFromText(); // GemStoneAddition - validation of text file
    }
    else {
      FileInputStream fos = new FileInputStream(args[0]);
      classes = parse(fos);
    }
    
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < classes.length; i++) {
      ClassMap cm = classes[i];
      sb.append(cm.getClassName());
      sb.append('/');
      sb.append(cm.getDescription());
      sb.append('/');
      sb.append(cm.getPreload());
      sb.append('/');
      sb.append(cm.getMagicNumber());
      sb.append('\n');
    }
    System.out.println(sb);
  }

  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * Receives SAX events when parsing the XML data.
   *
   * @author David Whitlock
   *
   */
  static class XmlHandler extends DefaultHandler  {
    
    /** Not parsing a protocol */
    private static final int INVALID_STATE = 0;

    /** Currently parsing a "description" element */
    private static final int DESCRIPTION_STATE = 1;

    /** Currently parsing a "class-name" element */
    private static final int CLASS_NAME_STATE = 2;

    /** Currently parsing a "preload" element */
    private static final int PRELOAD_STATE = 3;

    /** Currently parsing a "magic-number" element */
    private static final int MAGIC_NUMBER_STATE = 4;

    ////////////////////  Instance Fields  ////////////////////

    /** List of ClassMaps parsed from XML */
    private List classMaps = new ArrayList();

    /** The element that we are currently parsing */
    private int state;

    /** The description of the ckass we are currently parsing */
    private String description;

    /** Name of the class we are currently parsing */
    private String className;

    /** Do we preload the class we are currenlty parsing? */
    private boolean preload;

    /** The magic number of class we are currently parsing */
    private int magicNumber;

    /** An EntityResolver used for finding the DTD */
    private EntityResolver resolver = new ClassPathEntityResolver();

    ////////////////////  Accessor Methods  ////////////////////

    /**
     * Returns the class maps that are created when parsing the XML data
     */
    public ClassMap[] getClassMaps() {
      return (ClassMap[]) this.classMaps.toArray(new ClassMap[0]);
    }

    //////////////////////  Error Handling  //////////////////////

    /**
     * We simply ignore warnings
     */
    @Override // GemStoneAddition
    public void warning(SAXParseException exception)
      throws SAXException {

      return;
    }

    /**
     * Errors are re-thrown
     */
    @Override // GemStoneAddition
    public void error(SAXParseException exception)
      throws SAXException {

      throw exception;
    }

    /**
     * Fatal errors are re-thrown
     */
    @Override // GemStoneAddition
    public void fatalError(SAXParseException exception)
      throws SAXException {

      throw exception;
    }

    ////////////////////  Parsing Methods  ////////////////////

    /**
     * Handle the element accordingly
     */
    @Override // GemStoneAddition
    public void startElement(String namespaceURI, String localName,
                             String qName, Attributes atts)
      throws SAXException {

      if (qName.equals("description")) {
        this.state = DESCRIPTION_STATE;

      } else if (qName.equals("class-name")) {
        this.state = CLASS_NAME_STATE;
        
      } else if (qName.equals("preload")) {
        this.state = PRELOAD_STATE;

      } else if (qName.equals("magic-number")) {
        this.state = MAGIC_NUMBER_STATE;
      }
    }

    /**
     * Changes some state when we reach the end of certain elements
     */
    @Override // GemStoneAddition
    public void endElement(String namespaceURI, String localName,
                           String qName) throws SAXException {

      if (qName.equals("class")) {
        // Create a ClassMap object
        ClassMap classMap =
          new ClassMap(this.className, this.description, this.preload,
                       this.magicNumber);
        this.classMaps.add(classMap);

        // Reset state
        this.className = null;
        this.description = null;
        this.state = INVALID_STATE;

      } else {
        this.state = INVALID_STATE;
      }
    }

    /**
     * Handles text for the element we are currently parsing
     */
    @Override // GemStoneAddition
    public void characters(char[] ch, int start, int length)
      throws SAXException {
      
      switch (this.state) {
      case CLASS_NAME_STATE:
        this.className = new String(ch, start, length);
        break;
      case DESCRIPTION_STATE:
        this.description = new String(ch, start, length);
        break;
      case PRELOAD_STATE: {
        String s = new String(ch, start, length);
        this.preload = Boolean.valueOf(s).booleanValue();
        break;
      }
      case MAGIC_NUMBER_STATE: {
        String s = new String(ch, start, length);
        try {
          this.magicNumber = Integer.parseInt(s); 

        } catch (NumberFormatException ex) {
          throw new SAXException("Invalid magic number: " + s);
        }
      }
      break; // GemStoneAddition -- lint
      
      default:
        // Ignore
        break;
      }
    }

    /**
     * Delegate to a {@link ClassPathEntityResolver} to do the heavy
     * lifting. 
     */
    @Override // GemStoneAddition
    public InputSource resolveEntity(java.lang.String publicId,
                                     java.lang.String systemId)
      throws SAXException {

      try {
        return this.resolver.resolveEntity(publicId, systemId);

      } catch (java.io.IOException ex) {
        String s = "While resolving " + publicId + ", " + systemId;
        throw new SAXException(s, ex);
      }
    }
  }

}

