package com.gemstone.gemfire.internal.process;

import java.io.File;

import com.gemstone.gemfire.internal.process.ProcessController.Arguments;

/**
 * Defines {@link ProcessController} {@link Arguments} that must be implemented
 * to support the {@link FileProcessController}.
 *  
 * @author Kirk Lund
 * @since 8.0
 */
interface FileControllerParameters extends Arguments {
  public File getPidFile();
  public File getWorkingDirectory();
}
