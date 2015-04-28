package com.gemstone.gemfire.management.internal.cli.multistep;

import java.io.Serializable;

/**
 * Marker interface to identify remote steps from local steps
 * 
 * Command has to populate the right context information in
 * Remote step to get execution.
 * 
 * For state-ful interactive commands like select where steps
 * are iterating through the result(the state) to and fro, first
 * step has to create the state on the manager.
 * 
 * @author tushark
 */
public interface CLIRemoteStep extends CLIStep, Serializable{

}
