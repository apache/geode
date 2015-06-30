package com.gemstone.gemfire.test.process;

import java.util.List;

/**
 * Reads the stdout and stderr from a running process and stores then for test 
 * validation. Also provides a mechanism to waitFor the process to terminate. 
 * Extracted from ProcessWrapper.
 * 
 * @author Kirk Lund
 */
public class ProcessOutputReader {

  private static final long PROCESS_TIMEOUT_MILLIS = 10 * 60 * 1000L; // 10 minutes

  private int exitCode;
  
  private final Process p;
  private final ProcessStreamReader stdout;
  private final ProcessStreamReader stderr;
  private final List<String> lines;
  
  public ProcessOutputReader(final Process p, final ProcessStreamReader stdout, final ProcessStreamReader stderr, final List<String> lines) {
    this.p = p;
    this.stdout = stdout;
    this.stderr = stderr;
    this.lines = lines;
  }
  
  public void waitFor() {
    stdout.start();
    stderr.start();

    long startMillis = System.currentTimeMillis();
    try {
      stderr.join(PROCESS_TIMEOUT_MILLIS);
    } catch (Exception ignore) {
    }

    long timeLeft = System.currentTimeMillis() + PROCESS_TIMEOUT_MILLIS - startMillis;
    try {
      stdout.join(timeLeft);
    } catch (Exception ignore) {
    }

    this.exitCode = 0;
    int retryCount = 9;
    while (retryCount > 0) {
      retryCount--;
      try {
        exitCode = p.exitValue();
        break;
      } catch (IllegalThreadStateException e) {
        // due to bugs in Process we may not be able to get
        // a process's exit value.
        // We can't use Process.waitFor() because it can hang forever
        if (retryCount == 0) {
          if (stderr.linecount > 0) {
            // The process wrote to stderr so manufacture
            // an error exist code
            synchronized (lines) {
              lines.add("Failed to get exit status and it wrote"
                  + " to stderr so setting exit status to 1.");
            }
            exitCode = 1;
          }
        } else {
          // We need to wait around to give a chance for
          // the child to be reaped.See bug 19682
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
          }
        }
      }
    }
  }

  public int getExitCode() {
    return exitCode;
  }
}
