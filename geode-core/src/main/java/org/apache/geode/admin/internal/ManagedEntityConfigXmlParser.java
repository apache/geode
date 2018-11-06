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

import java.io.InputStream;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.geode.admin.AdminXmlException;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.internal.Assert;

/**
 * Parses an XML file and configures a {@link DistributedSystemConfig} from it.
 *
 * @since GemFire 4.0
 */
public class ManagedEntityConfigXmlParser extends ManagedEntityConfigXml implements ContentHandler {

  /** The <code>DistributedSystemConfig</code> to be configured */
  private DistributedSystemConfig config;

  /** The stack of intermediate values used while parsing */
  private Stack stack = new Stack();

  ////////////////////// Static Methods //////////////////////

  /**
   * Parses XML data and from it configures a <code>DistributedSystemConfig</code>.
   *
   * @throws AdminXmlException If an error is encountered while parsing the XML
   */
  public static void parse(InputStream is, DistributedSystemConfig config) {
    ManagedEntityConfigXmlParser handler = new ManagedEntityConfigXmlParser();
    handler.config = config;

    try {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setValidating(true);
      SAXParser parser = factory.newSAXParser();
      parser.parse(is, new DefaultHandlerDelegate(handler));

    } catch (Exception ex) {
      if (ex instanceof AdminXmlException) {
        throw (AdminXmlException) ex;

      } else if (ex.getCause() instanceof AdminXmlException) {
        throw (AdminXmlException) ex.getCause();

      } else if (ex instanceof SAXException) {
        // Silly JDK 1.4.2 XML parser wraps RunTime exceptions in a
        // SAXException. Pshaw!

        SAXException sax = (SAXException) ex;
        Exception cause = sax.getException();
        if (cause instanceof AdminXmlException) {
          throw (AdminXmlException) cause;
        }
      }

      throw new AdminXmlException(
          "While parsing XML", ex);
    }
  }

  /**
   * Helper method for parsing an integer
   *
   * @throws org.apache.geode.cache.CacheXmlException If <code>s</code> is a malformed integer
   */
  private static int parseInt(String s) {
    try {
      return Integer.parseInt(s);

    } catch (NumberFormatException ex) {
      throw new AdminXmlException(
          String.format("Malformed integer %s", s),
          ex);
    }
  }

  ////////////////////// Instance Methods //////////////////////

  public void startElement(String namespaceURI, String localName, String qName, Attributes atts)
      throws SAXException {

    if (qName.equals(DISTRIBUTED_SYSTEM)) {
      startDistributedSystem(atts);

    } else if (qName.equals(REMOTE_COMMAND)) {
      startRemoteCommand(atts);

    } else if (qName.equals(LOCATORS)) {
      startLocators(atts);

    } else if (qName.equals(MULTICAST)) {
      startMulticast(atts);

    } else if (qName.equals(LOCATOR)) {
      startLocator(atts);

    } else if (qName.equals(HOST)) {
      startHost(atts);

    } else if (qName.equals(WORKING_DIRECTORY)) {
      startWorkingDirectory(atts);

    } else if (qName.equals(PRODUCT_DIRECTORY)) {
      startProductDirectory(atts);

    } else if (qName.equals(SSL)) {
      startSSL(atts);

    } else if (qName.equals(PROTOCOLS)) {
      startProtocols(atts);

    } else if (qName.equals(CIPHERS)) {
      startCiphers(atts);

    } else if (qName.equals(PROPERTY)) {
      startProperty(atts);

    } else if (qName.equals(KEY)) {
      startKey(atts);

    } else if (qName.equals(VALUE)) {
      startValue(atts);

    } else if (qName.equals(CACHE_SERVER)) {
      startCacheServer(atts);

    } else if (qName.equals(CLASSPATH)) {
      startClassPath(atts);

    } else {
      throw new AdminXmlException(
          String.format("Unknown XML element %s",
              qName));
    }
  }

  public void endElement(String namespaceURI, String localName, String qName) throws SAXException {

    if (qName.equals(DISTRIBUTED_SYSTEM)) {
      endDistributedSystem();

    } else if (qName.equals(REMOTE_COMMAND)) {
      endRemoteCommand();

    } else if (qName.equals(LOCATORS)) {
      endLocators();

    } else if (qName.equals(MULTICAST)) {
      endMulticast();

    } else if (qName.equals(LOCATOR)) {
      endLocator();

    } else if (qName.equals(HOST)) {
      endHost();

    } else if (qName.equals(WORKING_DIRECTORY)) {
      endWorkingDirectory();

    } else if (qName.equals(PRODUCT_DIRECTORY)) {
      endProductDirectory();

    } else if (qName.equals(SSL)) {
      endSSL();

    } else if (qName.equals(PROTOCOLS)) {
      endProtocols();

    } else if (qName.equals(CIPHERS)) {
      endCiphers();

    } else if (qName.equals(PROPERTY)) {
      endProperty();

    } else if (qName.equals(KEY)) {
      endKey();

    } else if (qName.equals(VALUE)) {
      endValue();

    } else if (qName.equals(CACHE_SERVER)) {
      endCacheServer();

    } else if (qName.equals(CLASSPATH)) {
      endClassPath();

    } else {
      throw new AdminXmlException(
          String.format("Unknown XML element %s",
              qName));
    }
  }

  /**
   * When a <code>distributed-system</code> element is encountered, we push the
   * <code>DistributedSystemConfig</code> on the stack.
   */
  private void startDistributedSystem(Attributes atts) {
    Assert.assertTrue(stack.isEmpty());

    String id = atts.getValue(ID);
    if (id != null) {
      this.config.setSystemId(id);
    }

    String disable_tcp = atts.getValue(DISABLE_TCP);
    if (disable_tcp != null) {
      this.config.setDisableTcp(DISABLE_TCP.equalsIgnoreCase("true"));
    }

    stack.push(this.config);
  }

  /**
   * When a <code>distributed-system</code> element is finished
   */
  private void endDistributedSystem() {

  }

  /**
   * When a <code>multicast</code> is first encountered, get the
   * <code>DistributedSystemConfig</code> off of the top of the stack and set its multicast config
   * appropriately.
   */
  private void startMulticast(Attributes atts) {
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();

    String port = atts.getValue(PORT);
    config.setMcastPort(parseInt(port));

    String address = atts.getValue(ADDRESS);
    if (address != null) {
      config.setMcastAddress(address);
    }
  }

  private void endMulticast() {

  }

  /**
   * Starts a <code>remote-command</code> element. The item on top of the stack may be a
   * <code>DistributedSystemConfig</code> or it might be a <code>ManagedEntityConfig</code>.
   */
  private void startRemoteCommand(Attributes atts) {

  }

  /**
   * Ends a <code>remote-command</code> element. Pop the command off the top of the stack and set it
   * on the <code>DistributedSystemConfig</code> or it might be a <code>ManagedEntityConfig</code>
   * on top of the stack.
   */
  private void endRemoteCommand() {
    String remoteCommand = popString();
    Object top = stack.peek();
    Assert.assertTrue(top != null);

    if (top instanceof DistributedSystemConfig) {
      ((DistributedSystemConfig) top).setRemoteCommand(remoteCommand);

    } else if (top instanceof ManagedEntityConfig) {
      ((ManagedEntityConfig) top).setRemoteCommand(remoteCommand);

    } else {
      String s = "Did not expect a " + top.getClass().getName() + " on top of the stack";
      Assert.assertTrue(false, s);
    }
  }

  private void startLocators(Attributes atts) {

  }

  private void endLocators() {

  }

  private void startLocator(Attributes atts) {
    String port = atts.getValue(PORT);

    DistributedSystemConfig system = (DistributedSystemConfig) stack.peek();
    system.setMcastPort(0);

    DistributionLocatorConfig config = system.createDistributionLocatorConfig();

    config.setPort(parseInt(port));

    stack.push(config);
  }

  private void endLocator() {
    Object o = stack.pop();
    Assert.assertTrue(o instanceof DistributionLocatorConfig);
  }

  private void startHost(Attributes atts) {

  }

  /**
   * We assume that there is a <code>ManagedEntityConfig</code> on top of the stack.
   */
  private void endHost() {
    String host = popString();
    ManagedEntityConfig config = (ManagedEntityConfig) stack.peek();
    config.setHost(host);
  }

  private void startWorkingDirectory(Attributes atts) {

  }

  private void endWorkingDirectory() {
    String workingDirectory = popString();
    ManagedEntityConfig config = (ManagedEntityConfig) stack.peek();
    config.setWorkingDirectory(workingDirectory);
  }

  private void startProductDirectory(Attributes atts) {

  }

  private void endProductDirectory() {
    String productDirectory = popString();
    ManagedEntityConfig config = (ManagedEntityConfig) stack.peek();
    config.setProductDirectory(productDirectory);
  }

  private void startSSL(Attributes atts) {
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();
    config.setSSLEnabled(true);

    String authenticationRequired = atts.getValue(AUTHENTICATION_REQUIRED);
    config.setSSLAuthenticationRequired(Boolean.valueOf(authenticationRequired).booleanValue());
  }

  private void endSSL() {

  }

  private void startProtocols(Attributes atts) {

  }

  private void endProtocols() {
    String protocols = popString();
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();
    config.setSSLProtocols(protocols);
  }

  private void startCiphers(Attributes atts) {

  }

  private void endCiphers() {
    String ciphers = popString();
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();
    config.setSSLCiphers(ciphers);
  }

  private void startProperty(Attributes atts) {

  }

  private void endProperty() {
    String value = popString();
    String key = popString();
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();
    config.addSSLProperty(key, value);
  }

  private void startKey(Attributes atts) {

  }

  private void endKey() {
    String key = popString();
    stack.push(key);
  }

  private void startValue(Attributes atts) {

  }

  private void endValue() {
    String value = popString();
    stack.push(value);
  }

  private void startCacheServer(Attributes atts) {
    DistributedSystemConfig config = (DistributedSystemConfig) stack.peek();
    CacheServerConfig server = config.createCacheServerConfig();
    stack.push(server);
  }

  private void endCacheServer() {
    /* CacheServerConfig server = (CacheServerConfig) */ stack.pop();
  }

  private void startClassPath(Attributes atts) {

  }

  private void endClassPath() {
    String classpath = popString();
    CacheServerConfig server = (CacheServerConfig) stack.peek();
    server.setClassPath(classpath);
  }

  /**
   * Pops a <code>String</code> off of the stack.
   */
  private String popString() {
    Object o = stack.pop();

    if (o instanceof StringBuffer) {
      StringBuffer sb = (StringBuffer) o;
      return sb.toString();

    } else {
      return (String) o;
    }
  }

  /**
   * Long strings in XML files may generate multiple <code>characters</code> callbacks. Coalesce
   * multiple callbacks into one big string by using a <code>StringBuffer</code>. See bug 32122.
   */
  public void characters(char[] ch, int start, int length) throws SAXException {

    Object top = stack.peek();

    StringBuffer sb;
    if (top instanceof StringBuffer) {
      sb = (StringBuffer) top;

    } else {
      sb = new StringBuffer();
      stack.push(sb);
    }

    sb.append(ch, start, length);
  }

  ////////// Inherited methods that don't do anything //////////

  public void setDocumentLocator(Locator locator) {}

  public void startDocument() throws SAXException {}

  public void endDocument() throws SAXException {}

  public void startPrefixMapping(String prefix, String uri) throws SAXException {}

  public void endPrefixMapping(String prefix) throws SAXException {}

  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

  public void processingInstruction(String target, String data) throws SAXException {}

  public void skippedEntity(String name) throws SAXException {}

  /////////////////////// Inner Classes ///////////////////////

  /**
   * Class that delegates all of the methods of a {@link DefaultHandler} to a
   * {@link ManagedEntityConfigXmlParser} that implements all of the methods of
   * <code>DefaultHandler</code>, but <B>is not</B> a <code>DefaultHandler</code>.
   */
  static class DefaultHandlerDelegate extends DefaultHandler {
    /**
     * The <code>ManagedEntityConfigXmlParser</code> that does the real work
     */
    private ManagedEntityConfigXmlParser handler;

    /**
     * Creates a new <code>DefaultHandlerDelegate</code> that delegates to the given
     * <code>ManagedEntityConfigXmlParser</code>.
     */
    public DefaultHandlerDelegate(ManagedEntityConfigXmlParser handler) {
      this.handler = handler;
    }

    @Override
    public InputSource resolveEntity(String publicId, String systemId) throws SAXException {
      return handler.resolveEntity(publicId, systemId);
    }

    @Override
    public void setDocumentLocator(Locator locator) {
      handler.setDocumentLocator(locator);
    }

    @Override
    public void startDocument() throws SAXException {
      handler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
      handler.endDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
      handler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
      handler.endPrefixMapping(prefix);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
        throws SAXException {
      handler.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      handler.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      handler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
      handler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
      handler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
      handler.skippedEntity(name);
    }

    @Override
    public void warning(SAXParseException e) throws SAXException {
      handler.warning(e);
    }

    @Override
    public void error(SAXParseException e) throws SAXException {
      handler.error(e);
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
      handler.fatalError(e);
    }
  }

}
