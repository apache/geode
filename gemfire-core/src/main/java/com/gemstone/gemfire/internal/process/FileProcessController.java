package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.process.ControlFileWatchdog.ControlRequestHandler;
import com.gemstone.gemfire.lang.AttachAPINotFoundException;

/**
 * Controls a {@link ControllableProcess} using files to communicate between
 * processes.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public final class FileProcessController implements ProcessController {
  private static final Logger logger = LogService.getLogger();

  public static final String STATUS_TIMEOUT_PROPERTY = "gemfire.FileProcessController.STATUS_TIMEOUT";
  private final long statusTimeout = Long.getLong(STATUS_TIMEOUT_PROPERTY, 60*1000);
  
  private final FileControllerParameters arguments;
  private final int pid;

  /**
   * Constructs an instance for controlling a local process.
   * 
   * @param pid process id identifying the process to attach to
   * 
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  public FileProcessController(final FileControllerParameters arguments, final int pid) {
    if (pid < 1) {
      throw new IllegalArgumentException("Invalid pid '" + pid + "' specified");
    }
    this.pid = pid;
    this.arguments = arguments;
  }

  @Override
  public int getProcessId() {
    return this.pid;
  }
  
  @Override
  public String status() throws UnableToControlProcessException, IOException, InterruptedException, TimeoutException {
    return status(this.arguments.getWorkingDirectory(), this.arguments.getProcessType().getStatusRequestFileName(), this.arguments.getProcessType().getStatusFileName());
  }

  @Override
  public void stop() throws UnableToControlProcessException, IOException {
    stop(this.arguments.getWorkingDirectory(), this.arguments.getProcessType().getStopRequestFileName());
  }
  
  @Override
  public void checkPidSupport() {
    throw new AttachAPINotFoundException(LocalizedStrings.Launcher_ATTACH_API_NOT_FOUND_ERROR_MESSAGE.toLocalizedString());
  }
  
  private void stop(final File workingDir, final String stopRequestFileName) throws UnableToControlProcessException, IOException {
    final File stopRequestFile = new File(workingDir, stopRequestFileName);
    if (!stopRequestFile.exists()) {
      stopRequestFile.createNewFile();
    }
  }
  
  private String status(final File workingDir, final String statusRequestFileName, final String statusFileName) throws UnableToControlProcessException, IOException, InterruptedException, TimeoutException {
    // monitor for statusFile
    final File statusFile = new File(workingDir, statusFileName);
    final AtomicReference<String> statusRef = new AtomicReference<String>();
    
    final ControlRequestHandler statusHandler = new ControlRequestHandler() {
      @Override
      public void handleRequest() throws IOException {
        // read the statusFile
        final BufferedReader reader = new BufferedReader(new FileReader(statusFile));
        try {
          final StringBuilder lines = new StringBuilder();
          String line = null;
          while ((line = reader.readLine()) != null) {
            lines.append(line);
          }
          statusRef.set(lines.toString());
        } finally {
          reader.close();
        }
      }
    };

    final ControlFileWatchdog statusFileWatchdog = new ControlFileWatchdog(workingDir, statusFileName, statusHandler, true);
    statusFileWatchdog.start();

    final File statusRequestFile = new File(workingDir, statusRequestFileName);
    if (!statusRequestFile.exists()) {
      statusRequestFile.createNewFile();
    }
    
    // if timeout invoke stop and then throw TimeoutException
    final long start = System.currentTimeMillis();
    while (statusFileWatchdog.isAlive()) {
      Thread.sleep(100);
      if (System.currentTimeMillis() >= start + this.statusTimeout) {
        final TimeoutException te = new TimeoutException("Timed out waiting for process to create " + statusFile);
        try {
          statusFileWatchdog.stop();
        } catch (InterruptedException e) {
          logger.info("Interrupted while stopping status file watchdog.", e);
        } catch (RuntimeException e) {
          logger.info("Unexpected failure while stopping status file watchdog.", e);
        }
        throw te;
      }
    }
    assert statusRef.get() != null;
    return statusRef.get();
  }
}
