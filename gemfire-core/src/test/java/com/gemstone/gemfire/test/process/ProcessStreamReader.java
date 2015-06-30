package com.gemstone.gemfire.test.process;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Queue;

/**
 * Reads the output from a stream and stores it for test validation. Extracted
 * from ProcessWrapper.
 * 
 * @author Kirk Lund
 */
public class ProcessStreamReader extends Thread {
  
  private volatile Throwable startStack;
  private final String command;
  private final BufferedReader reader;
  private final Queue<String> lineBuffer;
  private final List<String> allLines;

  public int linecount = 0;

  public ProcessStreamReader(String command, InputStream stream, Queue<String> lineBuffer, List<String> allLines) {
    this.command = command;
    this.reader = new BufferedReader(new InputStreamReader(stream));
    this.lineBuffer = lineBuffer;
    this.allLines = allLines;
  }

  public Throwable getStart() {
    return this.startStack;
  }
  
  public Throwable getFailure() {
    if (this.startStack.getCause() != null) {
      return this.startStack.getCause();
    } else {
      return null;
    }
  }
  
  @Override
  public void start() {
    this.startStack = new Throwable();
    super.start();
  }
  
  @Override
  public void run() {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        linecount++;
        lineBuffer.offer(line);
        allLines.add(line);
      }

      // EOF
      reader.close();
    } catch (Exception cause) {
      this.startStack.initCause(cause);
      System.err.println("ProcessStreamReader: Failure while reading from child process: " + this.command + " " + this.startStack.getMessage());
      this.startStack.printStackTrace();
    }
  }
}
