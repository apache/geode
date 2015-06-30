package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Reads the output stream of a Process. Default implementation performs blocking 
 * per-line reading of the InputStream.
 * 
 * Extracted from ProcessStreamReader.
 * 
 * @author Kirk Lund
 * @since 8.2
 */
public final class BlockingProcessStreamReader extends ProcessStreamReader {
  private static final Logger logger = LogService.getLogger();

  protected BlockingProcessStreamReader(final Builder builder) {
    super(builder);
  }

  @Override
  public void run() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Running {}", this);
    }
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = reader.readLine()) != null) {
        this.inputListener.notifyInputLine(line);
      }
    } catch (IOException e) {
      if (isDebugEnabled) {
        logger.debug("Failure reading from buffered input stream: {}", e.getMessage(), e);
      }
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        if (isDebugEnabled) {
          logger.debug("Failure closing buffered input stream reader: {}", e.getMessage(), e);
        }
      }
      if (isDebugEnabled) {
        logger.debug("Terminating {}", this);
      }
    }
  }
}
