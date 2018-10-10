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
package org.apache.geode.admin.internal;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.CacheServer;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.internal.Assert;

/**
 * Generates XML data that represents the managed entities in an
 * <code>AdminDistributedSystem</code>. This class is used mainly for testing.
 *
 * @since GemFire 4.0
 */
public class ManagedEntityConfigXmlGenerator extends ManagedEntityConfigXml implements XMLReader {

  /** An empty <code>Attributes</code> */
  private static Attributes EMPTY = new AttributesImpl();

  ///////////////////////// Instance Fields ////////////////////////

  /**
   * The <code>AdminDistributedSystem</code> for which we are generating XML
   */
  private AdminDistributedSystem system;

  /** The content handler to which SAX events are generated */
  private ContentHandler handler;

  ///////////////////////// Static Methods ////////////////////////

  /**
   * Generates an XML representation of all of the managed entities in the given
   * <code>AdminDistributedSystem</code>.
   */
  public static void generate(AdminDistributedSystem system, PrintWriter pw) {
    (new ManagedEntityConfigXmlGenerator(system)).generate(pw);
  }

  ///////////////////////// Constructors //////////////////////////

  /**
   * Creates a new generator for the given <code>AdminDistributedSystem</code>.
   */
  private ManagedEntityConfigXmlGenerator(AdminDistributedSystem system) {
    this.system = system;
  }

  /////////////////////// Instance Methods ///////////////////////

  /**
   * Generates XML and writes it to the given <code>PrintWriter</code>
   */
  private void generate(PrintWriter pw) {
    // Use JAXP's transformation API to turn SAX events into pretty
    // XML text
    try {
      Source src = new SAXSource(this, new InputSource());
      Result res = new StreamResult(pw);

      TransformerFactory xFactory = TransformerFactory.newInstance();
      Transformer xform = xFactory.newTransformer();
      xform.setOutputProperty(OutputKeys.METHOD, "xml");
      xform.setOutputProperty(OutputKeys.INDENT, "yes");
      xform.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, SYSTEM_ID);
      xform.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, PUBLIC_ID);
      xform.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      xform.transform(src, res);
      pw.flush();

    } catch (Exception ex) {
      RuntimeException ex2 = new RuntimeException(
          "Exception thrown while generating XML.");
      ex2.initCause(ex);
      throw ex2;
    }
  }

  /**
   * Called by the transformer to parse the "input source". We ignore the input source and, instead,
   * generate SAX events to the {@link #setContentHandler ContentHandler}.
   */
  public void parse(InputSource input) throws SAXException {
    Assert.assertTrue(this.handler != null);

    handler.startDocument();

    AttributesImpl atts = new AttributesImpl();

    atts.addAttribute("", "", ID, "", String.valueOf(this.system.getConfig().getSystemId()));

    handler.startElement("", DISTRIBUTED_SYSTEM, DISTRIBUTED_SYSTEM, atts);

    // Add generation methods here
    try {
      generateRemoteCommand();
      generateDiscovery();
      generateSSL();
      generateCacheServers();

    } catch (AdminException ex) {
      throw new SAXException(
          "An AdminException was thrown while generating XML.",
          ex);
    }

    handler.endElement("", DISTRIBUTED_SYSTEM, DISTRIBUTED_SYSTEM);
    handler.endDocument();
  }

  /**
   * Generates XML for the remote command
   */
  private void generateRemoteCommand() throws SAXException {
    String remoteCommand = this.system.getRemoteCommand();

    handler.startElement("", REMOTE_COMMAND, REMOTE_COMMAND, EMPTY);

    handler.characters(remoteCommand.toCharArray(), 0, remoteCommand.length());

    handler.endElement("", REMOTE_COMMAND, REMOTE_COMMAND);
  }

  /**
   * Generates XML for locators in the distributed system
   */
  private void generateDiscovery() throws SAXException {
    handler.startElement("", LOCATORS, LOCATORS, EMPTY);

    generateLocators();

    handler.endElement("", LOCATORS, LOCATORS);
  }

  /**
   * Generates XML for the distributed system's locators
   */
  private void generateLocators() throws SAXException {
    DistributionLocator[] locators = this.system.getDistributionLocators();
    for (int i = 0; i < locators.length; i++) {
      generateLocator(locators[i].getConfig());
    }
  }

  /**
   * Generates XML for a locator
   */
  private void generateLocator(DistributionLocatorConfig config) throws SAXException {

    AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "", PORT, "", String.valueOf(config.getPort()));

    handler.startElement("", LOCATOR, LOCATOR, atts);

    generateEntityConfig(config);

    handler.endElement("", LOCATOR, LOCATOR);
  }

  /**
   * Generates XML for attributes common to all managed entities.
   */
  private void generateEntityConfig(ManagedEntityConfig config) throws SAXException {

    String host = config.getHost();
    if (host != null) {
      handler.startElement("", HOST, HOST, EMPTY);
      handler.characters(host.toCharArray(), 0, host.length());
      handler.endElement("", HOST, HOST);
    }

    String remoteCommand = config.getRemoteCommand();
    if (remoteCommand != null) {
      handler.startElement("", REMOTE_COMMAND, REMOTE_COMMAND, EMPTY);
      handler.characters(remoteCommand.toCharArray(), 0, remoteCommand.length());
      handler.endElement("", REMOTE_COMMAND, REMOTE_COMMAND);
    }

    String workingDirectory = config.getWorkingDirectory();
    if (workingDirectory != null) {
      handler.startElement("", WORKING_DIRECTORY, WORKING_DIRECTORY, EMPTY);
      handler.characters(workingDirectory.toCharArray(), 0, workingDirectory.length());
      handler.endElement("", WORKING_DIRECTORY, WORKING_DIRECTORY);
    }

    String productDirectory = config.getProductDirectory();
    if (productDirectory != null) {
      handler.startElement("", PRODUCT_DIRECTORY, PRODUCT_DIRECTORY, EMPTY);
      handler.characters(productDirectory.toCharArray(), 0, productDirectory.length());
      handler.endElement("", PRODUCT_DIRECTORY, PRODUCT_DIRECTORY);
    }
  }

  /**
   * Generates XML for the SSL configuration of the distributed system.
   */
  private void generateSSL() throws SAXException {
    DistributedSystemConfig config = this.system.getConfig();

    boolean sslEnabled = config.isSSLEnabled();
    if (!sslEnabled) {
      return;
    }

    AttributesImpl atts = new AttributesImpl();
    atts.addAttribute("", "", AUTHENTICATION_REQUIRED, "",
        String.valueOf(config.isSSLAuthenticationRequired()));

    handler.startElement("", SSL, SSL, atts);

    String protocols = config.getSSLProtocols();
    if (protocols != null) {
      handler.startElement("", PROTOCOLS, PROTOCOLS, EMPTY);
      handler.characters(protocols.toCharArray(), 0, protocols.length());
      handler.endElement("", PROTOCOLS, PROTOCOLS);
    }

    String ciphers = config.getSSLCiphers();
    if (ciphers != null) {
      handler.startElement("", CIPHERS, CIPHERS, EMPTY);
      handler.characters(ciphers.toCharArray(), 0, ciphers.length());
      handler.endElement("", CIPHERS, CIPHERS);
    }

    Properties sslProps = config.getSSLProperties();
    for (Iterator iter = sslProps.entrySet().iterator(); iter.hasNext();) {
      Map.Entry entry = (Map.Entry) iter.next();
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();

      handler.startElement("", PROPERTY, PROPERTY, EMPTY);

      handler.startElement("", KEY, KEY, EMPTY);
      handler.characters(key.toCharArray(), 0, key.length());
      handler.endElement("", KEY, KEY);

      handler.startElement("", VALUE, VALUE, EMPTY);
      handler.characters(value.toCharArray(), 0, value.length());
      handler.endElement("", VALUE, VALUE);

      handler.endElement("", PROPERTY, PROPERTY);
    }

    handler.endElement("", SSL, SSL);
  }

  /**
   * Generates an XML representation of the <code>CacheServer</code>s in the distributed system.
   */
  private void generateCacheServers() throws SAXException, AdminException {

    CacheServer[] servers = this.system.getCacheServers();
    for (int i = 0; i < servers.length; i++) {
      generateCacheServer(servers[i].getConfig());
    }
  }

  /**
   * Generates an XML representation of a <code>CacheServerConfig</code>.
   */
  private void generateCacheServer(CacheServerConfig config) throws SAXException {

    handler.startElement("", CACHE_SERVER, CACHE_SERVER, EMPTY);

    generateEntityConfig(config);

    String classpath = config.getClassPath();
    if (classpath != null) {
      handler.startElement("", CLASSPATH, CLASSPATH, EMPTY);
      handler.characters(classpath.toCharArray(), 0, classpath.length());
      handler.endElement("", CLASSPATH, CLASSPATH);
    }

    handler.endElement("", CACHE_SERVER, CACHE_SERVER);
  }

  /**
   * Keep track of the content handler for use during {@link #parse(String)}.
   */
  public void setContentHandler(ContentHandler handler) {
    this.handler = handler;
  }

  public ContentHandler getContentHandler() {
    return this.handler;
  }

  public ErrorHandler getErrorHandler() {
    return this;
  }

  ////////// Inherited methods that don't do anything //////////

  public boolean getFeature(String name)
      throws SAXNotRecognizedException, SAXNotSupportedException {
    return false;
  }

  public void setFeature(String name, boolean value)
      throws SAXNotRecognizedException, SAXNotSupportedException {

  }

  public Object getProperty(String name)
      throws SAXNotRecognizedException, SAXNotSupportedException {

    return null;
  }

  public void setProperty(String name, Object value)
      throws SAXNotRecognizedException, SAXNotSupportedException {

  }

  public void setEntityResolver(EntityResolver resolver) {

  }

  public EntityResolver getEntityResolver() {
    return this;
  }

  public void setDTDHandler(DTDHandler handler) {

  }

  public DTDHandler getDTDHandler() {
    return null;
  }

  public void setErrorHandler(ErrorHandler handler) {

  }

  public void parse(String systemId) throws IOException, SAXException {

  }


}
