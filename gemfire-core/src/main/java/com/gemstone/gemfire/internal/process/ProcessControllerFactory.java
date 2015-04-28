package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;

import com.gemstone.gemfire.internal.util.IOUtils;

/**
 * Manages which implementation of {@link ProcessController} will be used and
 * constructs the instance.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public final class ProcessControllerFactory {

  public static final String PROPERTY_DISABLE_ATTACH_API = "gemfire.test.ProcessControllerFactory.DisableAttachApi";
  
  private final boolean disableAttachApi;
  
  public ProcessControllerFactory() {
    this.disableAttachApi = Boolean.getBoolean(PROPERTY_DISABLE_ATTACH_API);
  }
  
  public ProcessController createProcessController(final ProcessControllerParameters arguments, final int pid) {
    if (pid < 1) {
      throw new IllegalArgumentException("Invalid pid '" + pid + "' specified");
    }
    try {
      if (isAttachAPIFound()) {
        return new MBeanProcessController((MBeanControllerParameters)arguments, pid);
      } else {
        return new FileProcessController((FileControllerParameters)arguments, pid);
      }
    } catch (final ExceptionInInitializerError e) {
      //LOGGER.warn("Attach API class not found", e);
    }
    return null;
  }
  
  public ProcessController createProcessController(final ProcessControllerParameters arguments, final File pidFile) throws IOException {
    return createProcessController(arguments, readPid(pidFile));
  }
  
  public ProcessController createProcessController(final ProcessControllerParameters arguments, final File directory, final String pidFilename) throws IOException {
    return createProcessController(arguments, readPid(directory, pidFilename));
  }

  public boolean isAttachAPIFound() {
    if (this.disableAttachApi) {
      return false;
    }
    boolean found = false;
    try {
      final Class<?> virtualMachineClass = Class.forName("com.sun.tools.attach.VirtualMachine");
      found = virtualMachineClass != null;
    } catch (ClassNotFoundException e) {
    }
    return found;
  }
  
  /**
   * Reads in the pid from the specified file.
   * 
   * @param pidFile the file containing the pid of the process to stop
   * 
   * @return the process id (pid) contained within the pidFile
   * 
   * @throws IllegalArgumentException if the pid in the pidFile is not a positive integer
   * @throws IOException if unable to read from the specified file
   * @throws NumberFormatException if the pid file does not contain a parsable integer
   */
  private static int readPid(File pidFile) throws IOException {
    BufferedReader fileReader = null;
    String pidValue = null;

    try {
      fileReader = new BufferedReader(new FileReader(pidFile));
      pidValue = fileReader.readLine();

      final int pid = Integer.parseInt(pidValue);

      if (pid < 1) {
        throw new IllegalArgumentException("Invalid pid '" + pid + "' found in " + pidFile.getCanonicalPath());
      }

      return pid;
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid pid '" + pidValue + "' found in " + pidFile.getCanonicalPath());
    }
    finally {
      IOUtils.close(fileReader);
    }
  }

  /**
   * Reads in the pid from the named file contained within the specified
   * directory.
   * 
   * @param directory directory containing a file of name pidFileName
   * @param pidFilename name of the file containing the pid of the process to stop
   * 
   * @return the process id (pid) contained within the pidFile
   * 
   * @throws FileNotFoundException if the specified file name is not found within the directory
   * @throws IllegalArgumentException if the pid in the pidFile is not a positive integer
   * @throws IllegalStateException if dir is not an existing directory
   * @throws IOException if an I/O error occurs
   * @throws NumberFormatException if the pid file does not contain a parsable integer
   */
  private static int readPid(final File directory, final String pidFilename) throws IOException {
    if (!directory.isDirectory() && directory.exists()) {
      throw new IllegalArgumentException("Argument '" + directory + "' must be an existing directory!");
    }

    final File[] files = directory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String filename) {
        return filename.equals(pidFilename);
      }
    });

    if (files.length == 0) {
      throw new FileNotFoundException("Unable to find PID file '" + pidFilename + "' in directory " + directory);
    }

    return readPid(files[0]);
  }
}
