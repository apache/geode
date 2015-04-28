package com.gemstone.gemfire.internal.process;

/**
 * Defines the methods for providing input arguments to the <code>ProcessController</code>.
 * 
 * Implementations of <code>ProcessController</code> are in this package. Classes that
 * implement <code>ProcessControllerArguments</code> would typically be in a different
 * package.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public interface ProcessControllerParameters extends FileControllerParameters, MBeanControllerParameters {
}
