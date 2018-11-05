/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin.jmx.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.ExitCode;

/**
 * A tool that reads the XML description of MBeans used with the Jakarta Commons Modeler and
 * generates an HTML file that documents each MBean.
 *
 * @since GemFire 3.5
 */
public class GenerateMBeanHTML extends DefaultHandler {

  /** The location of the DTD for the MBean descriptions */
  private static final String DTD_LOCATION =
      "/org/apache/geode/admin/jmx/internal/doc-files/mbeans-descriptors.dtd";

  /** The name of the "mbean-descriptors" element */
  private static final String MBEANS_DESCRIPTORS = "mbeans-descriptors";

  /** The name of the "mbean" element */
  private static final String MBEAN = "mbean";

  /** The name of the "name" attribute */
  private static final String NAME = "name";

  /** The name of the "description" attribute */
  private static final String DESCRIPTION = "description";

  /** The name of the "type" attribute */
  private static final String TYPE = "type";

  /** The name of the "attribute" element */
  private static final String ATTRIBUTE = "attribute";

  /** The name of the "writeable" attribute */
  private static final String WRITEABLE = "writeable";

  /** The name of the "operation" element */
  private static final String OPERATION = "operation";

  /** The name of the "returnType" attribute */
  private static final String RETURN_TYPE = "returnType";

  /** The name of the "paremeter" element */
  private static final String PARAMETER = "parameter";

  /** The name of the "notification" element */
  private static final String NOTIFICATION = "notification";

  /** The name of the "field" element */
  private static final String FIELD = "field";

  /** The name of the "value" attribute */
  private static final String VALUE = "value";

  ////////////////////// Instance Fields ///////////////////////

  /** Where the generated HTML data is written */
  private PrintWriter pw;

  /** Have we seen attributes for the current MBean? */
  private boolean seenAttribute = false;

  /** Have we seen operations for the current MBean? */
  private boolean seenOperation = false;

  /** Have we seen notifications for the current MBean? */
  private boolean seenNotifications = false;

  /////////////////////// Static Methods ///////////////////////

  /**
   * Converts data from the given <code>InputStream</code> into HTML that is written to the given
   * <code>PrintWriter</code>
   */
  private static void convert(InputStream in, PrintWriter out) throws Exception {

    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(true);
    SAXParser parser = factory.newSAXParser();
    DefaultHandler handler = new GenerateMBeanHTML(out);
    parser.parse(in, handler);
  }

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>GenerateMBeanHTML</code> that writes to the given <code>PrintWriter</code>.
   */
  private GenerateMBeanHTML(PrintWriter pw) {
    this.pw = pw;
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Given a public id, attempt to resolve it to a DTD. Returns an <code>InputSoure</code> for the
   * DTD.
   */
  @Override
  public InputSource resolveEntity(String publicId, String systemId) throws SAXException {

    if (publicId == null || systemId == null) {
      throw new SAXException(String.format("Public Id: %s System Id: %s",
          new Object[] {publicId, systemId}));
    }

    // Figure out the location for the publicId.
    String location = DTD_LOCATION;

    InputSource result;
    {
      InputStream stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), location);
      if (stream != null) {
        result = new InputSource(stream);
      } else {
        throw new SAXNotRecognizedException(
            String.format("DTD not found: %s", location));
      }
    }

    return result;
  }

  /**
   * Warnings are ignored
   */
  @Override
  public void warning(SAXParseException ex) throws SAXException {

  }

  /**
   * Rethrow the <code>SAXParseException</code>
   */
  @Override
  public void error(SAXParseException ex) throws SAXException {
    throw ex;
  }

  /**
   * Rethrow the <code>SAXParseException</code>
   */
  @Override
  public void fatalError(SAXParseException ex) throws SAXException {
    throw ex;
  }

  /**
   * Starts the HTML document
   */
  private void startMBeansDescriptors() {
    pw.println("<HTML>");
    pw.println("<HEAD>");
    pw.println("<TITLE>GemFire MBeans Interface</TITLE>");
    pw.println("</HEAD>");
    pw.println("");
    pw.println("<h1>GemFire Management Beans</h1>");
    pw.println("");
    pw.println("<P>This document describes the attributes, operations,");
    pw.println("and notifications of the GemFire Administration");
    pw.println("Management Beans (MBeans).</P>");
    pw.println("");
  }

  /**
   * Ends the HTML document
   */
  private void endMBeansDescriptors() {
    pw.println("</HTML>");
  }

  /**
   * Generates a heading and a table declaration for an MBean
   */
  private void startMBean(Attributes atts) {
    String name = atts.getValue(NAME);
    /* String description = */ atts.getValue(DESCRIPTION);
    pw.println("<h2><b>" + name + "</b> MBean</h2>");
    pw.println("<table border=\"0\" cellpadding=\"3\">");
    pw.println("<tr valign=\"top\">");
    pw.println("  <th align=\"left\">Description:</th>");
    pw.println("  <td colspan=\"4\">GemFire distributed system</td>");
    pw.println("</tr>");
  }

  /**
   * Ends the MBean table
   */
  private void endMBean() {
    this.seenAttribute = false;
    this.seenOperation = false;
    this.seenNotifications = false;

    pw.println("</table>");
    pw.println("");

    pw.println("<P></P>");
    pw.println("");
  }

  /**
   * Generates a table row for an MBean attribute
   */
  private void startAttribute(Attributes atts) {
    if (!this.seenAttribute) {
      // Print header row
      pw.println("<tr valign=\"top\">");
      pw.println("  <th align=\"left\">Attributes</th>");
      pw.println("  <th align=\"left\" colspan=\"2\">Name</th>");
      pw.println("  <th align=\"left\">Type</th>");
      pw.println("  <th align=\"left\">Description</th>");
      pw.println("  <th align=\"left\">Writable</th>");
      pw.println("</tr>");

    }

    this.seenAttribute = true;

    String name = atts.getValue(NAME);
    String description = atts.getValue(DESCRIPTION);
    String type = atts.getValue(TYPE);
    String writeable = atts.getValue(WRITEABLE);

    pw.println("<tr valign=\"top\">");
    pw.println("  <td></td>");
    pw.println("  <td colspan=\"2\">" + name + "</td>");
    pw.println("  <td>" + type + "</td>");
    pw.println("  <td>" + description + "</td>");
    pw.println("  <td>" + writeable + "</td>");
    pw.println("</tr>");
  }

  /**
   * Generates a table row for an MBean operation
   */
  private void startOperation(Attributes atts) {
    if (!this.seenOperation) {
      if (!this.seenAttribute) {
        pw.println("<tr valign=\"top\">");
        pw.println("  <th align=\"left\">Operations</th>");
        pw.println("  <th align=\"left\" colspan=\"2\">Name</th>");
        pw.println("  <th align=\"left\">Type</th>");
        pw.println("  <th align=\"left\">Description</th>");
        pw.println("  <th align=\"left\"></th>");
        pw.println("</tr>");

      } else {
        String title = "Operations and Parameters";
        pw.println("<tr valign=\"top\">");
        pw.println("  <th align=\"left\" colspan=\"6\">" + title + "</th>");
        pw.println("</tr>");
      }
    }

    this.seenOperation = true;

    String name = atts.getValue(NAME);
    String type = atts.getValue(RETURN_TYPE);
    String description = atts.getValue(DESCRIPTION);

    pw.println("<tr valign=\"top\">");
    pw.println("  <td></td>");
    pw.println("  <td colspan=\"2\">" + name + "</td>");
    pw.println("  <td>" + type + "</td>");
    pw.println("  <td colspan=\"2\">" + description + "</td>");
    pw.println("</tr>");

  }

  /**
   * Generates a table row for the parameter of an MBean operation
   */
  private void startParameter(Attributes atts) {
    String name = atts.getValue(NAME);
    String description = atts.getValue(DESCRIPTION);
    String type = atts.getValue(TYPE);

    pw.println("<tr valign=\"top\">");
    pw.println("  <td></td>");
    pw.println("  <td width=\"10\"></td>");
    pw.println("  <td>" + name + "</td>");
    pw.println("  <td>" + type + "</td>");
    pw.println("  <td colspan=\"2\">" + description + "</td>");
    pw.println("</tr>");
  }

  /**
   * Generates a row in a table for an MBean notification
   */
  private void startNotification(Attributes atts) {
    if (!this.seenNotifications) {
      if (!this.seenAttribute && !this.seenOperation) {
        pw.println("<tr valign=\"top\">");
        pw.println("  <th align=\"left\">Notifications</th>");
        pw.println("  <th align=\"left\" colspan=\"2\">Name</th>");
        pw.println("  <th align=\"left\">Type</th>");
        pw.println("  <th align=\"left\">Description</th>");
        pw.println("  <th align=\"left\"></th>");
        pw.println("</tr>");
        pw.println("</tr>");

      } else {
        pw.println("<tr valign=\"top\">");
        pw.println("  <th align=\"left\" colspan=\"6\">Notifications and Fields</th>");
        pw.println("</tr>");
      }
    }

    this.seenNotifications = true;

    String name = atts.getValue(NAME);
    String description = atts.getValue(DESCRIPTION);

    pw.println("<tr valign=\"top\">");
    pw.println("  <td></td>");
    pw.println("  <td colspan=\"3\">" + name + "</td>");
    pw.println("  <td colspan=\"3\">" + description + "</td>");
    pw.println("</tr>");

  }

  /**
   * Generates a table row for a descriptor field
   */
  private void startField(Attributes atts) {
    String name = atts.getValue(NAME);
    String value = atts.getValue(VALUE);

    pw.println("<tr valign=\"top\">");
    pw.println("  <td></td>");
    pw.println("  <td width=\"10\"></td>");
    pw.println("  <td colspan=\"2\">" + name + "</td>");
    pw.println("  <td colspan=\"2\">" + value + "</td>");
    pw.println("</tr>");

  }

  @Override
  public void startElement(String namespaceURI, String localName, String qName, Attributes atts)
      throws SAXException {

    if (qName.equals(MBEANS_DESCRIPTORS)) {
      startMBeansDescriptors();

    } else if (qName.equals(MBEAN)) {
      startMBean(atts);

    } else if (qName.equals(ATTRIBUTE)) {
      startAttribute(atts);

    } else if (qName.equals(OPERATION)) {
      startOperation(atts);

    } else if (qName.equals(PARAMETER)) {
      startParameter(atts);

    } else if (qName.equals(NOTIFICATION)) {
      startNotification(atts);

    } else if (qName.equals(FIELD)) {
      startField(atts);
    }

  }


  @Override
  public void endElement(String namespaceURI, String localName, String qName) throws SAXException {

    if (qName.equals(MBEANS_DESCRIPTORS)) {
      endMBeansDescriptors();

    } else if (qName.equals(MBEAN)) {
      endMBean();
    }

  }

  ////////// Inherited methods that don't do anything //////////

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {

  }

  @Override
  public void setDocumentLocator(Locator locator) {}

  @Override
  public void startDocument() throws SAXException {}

  @Override
  public void endDocument() throws SAXException {}

  @Override
  public void startPrefixMapping(String prefix, String uri) throws SAXException {}

  @Override
  public void endPrefixMapping(String prefix) throws SAXException {}

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

  @Override
  public void processingInstruction(String target, String data) throws SAXException {}

  @Override
  public void skippedEntity(String name) throws SAXException {}

  //////////////////////// Main Program ////////////////////////

  private static final PrintStream err = System.err;

  /**
   * Prints usage information about this program
   */
  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java GenerateMBeanHTML xmlFile htmlFile");
    err.println("");
    err.println("Converts an MBeans description XML file into an HTML");
    err.println("file suitable for documentation");

    err.println("");

    ExitCode.FATAL.doSystemExit();
  }

  public static void main(String[] args) throws Exception {
    String xmlFileName = null;
    String htmlFileName = null;

    for (int i = 0; i < args.length; i++) {
      if (xmlFileName == null) {
        xmlFileName = args[i];

      } else if (htmlFileName == null) {
        htmlFileName = args[i];

      } else {
        usage("Extraneous command line argument: " + args[i]);
      }
    }

    if (xmlFileName == null) {
      usage("Missing XML file name");

    } else if (htmlFileName == null) {
      usage("Missing HTML file name");
    }

    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {
      usage("XML file \"" + xmlFile + "\" does not exist");
    }

    File htmlFile = new File(htmlFileName);
    convert(new FileInputStream(xmlFile), new PrintWriter(new FileWriter(htmlFile), true));
  }

}
