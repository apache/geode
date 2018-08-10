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
package org.apache.geode.security.templates;

import org.apache.logging.log4j.Logger;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.LogService;

/**
 * Implementation of {@link ErrorHandler} interface to handle validation errors while XML parsing.
 *
 * This throws back exceptions raised for {@code error} and {@code fatalError} cases while a
 * {@link LogWriter#warning(String)} level logging is done for the {@code warning} case.
 *
 * @since GemFire 5.5
 */
public class XmlErrorHandler implements ErrorHandler {

  private static final Logger logger = LogService.getLogger();

  private final LogWriter systemLogWriter;
  private final String xmlFileName;

  public XmlErrorHandler(final LogWriter systemLogWriter, final String xmlFileName) {
    this.systemLogWriter = systemLogWriter;
    this.xmlFileName = xmlFileName;
  }

  /**
   * Throws back the exception with the name of the XML file and the position where the exception
   * occurred.
   */
  @Override
  public void error(final SAXParseException exception) throws SAXException {
    throw new SAXParseException("Error while parsing XML at line " + exception.getLineNumber()
        + " column " + exception.getColumnNumber() + ": " + exception.getMessage(), null,
        exception);
  }

  /**
   * Throws back the exception with the name of the XML file and the position where the exception
   * occurred.
   */
  @Override
  public void fatalError(final SAXParseException exception) throws SAXException {
    throw new SAXParseException("Fatal error while parsing XML at line " + exception.getLineNumber()
        + " column " + exception.getColumnNumber() + ": " + exception.getMessage(), null,
        exception);
  }

  /**
   * Log the exception at {@link LogWriter#warning(String)} level with XML filename and the position
   * of exception in the file.
   */
  @Override
  public void warning(final SAXParseException exception) throws SAXException {
    this.systemLogWriter.warning(
        "Warning while parsing XML [" + this.xmlFileName + "] at line " + exception.getLineNumber()
            + " column " + exception.getColumnNumber() + ": " + exception.getMessage(),
        exception);
  }
}
