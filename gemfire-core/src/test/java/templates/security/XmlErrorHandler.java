/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.gemstone.gemfire.LogWriter;

/**
 * Implementation of {@link ErrorHandler} interface to handle validation errors
 * while XML parsing.
 * 
 * This throws back exceptions raised for <code>error</code> and
 * <code>fatalError</code> cases while a {@link LogWriter#warning(String)} level
 * logging is done for the <code>warning</code> case.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class XmlErrorHandler implements ErrorHandler {

  private LogWriter logger;

  private String xmlFileName;

  public XmlErrorHandler(LogWriter logger, String xmlFileName) {

    this.logger = logger;
    this.xmlFileName = xmlFileName;
  }

  /**
   * Throws back the exception with the name of the XML file and the position
   * where the exception occurred.
   */
  public void error(SAXParseException exception) throws SAXException {

    throw new SAXParseException("Error while parsing XML at line "
        + exception.getLineNumber() + " column " + exception.getColumnNumber()
        + ": " + exception.getMessage(), null);
  }

  /**
   * Throws back the exception with the name of the XML file and the position
   * where the exception occurred.
   */
  public void fatalError(SAXParseException exception) throws SAXException {

    throw new SAXParseException("Fatal error while parsing XML at line "
        + exception.getLineNumber() + " column " + exception.getColumnNumber()
        + ": " + exception.getMessage(), null);
  }

  /**
   * Log the exception at {@link LogWriter#warning(String)} level with XML
   * filename and the position of exception in the file.
   */
  public void warning(SAXParseException exception) throws SAXException {

    this.logger.warning("Warning while parsing XML [" + this.xmlFileName
        + "] at line " + exception.getLineNumber() + " column "
        + exception.getColumnNumber() + ": " + exception.getMessage());
  }

}
