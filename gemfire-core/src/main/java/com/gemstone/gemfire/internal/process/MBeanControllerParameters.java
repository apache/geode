package com.gemstone.gemfire.internal.process;

import javax.management.ObjectName;

import com.gemstone.gemfire.internal.process.ProcessController.Arguments;

/**
 * Defines {@link ProcessController} {@link Arguments} that must be implemented
 * to support the {@link MBeanProcessController}.

 * @author Kirk Lund
 * @since 8.0
 */
interface MBeanControllerParameters extends Arguments {
  public ObjectName getNamePattern();
  public String getPidAttribute();
  public String getStatusMethod();
  public String getStopMethod();
  public String[] getAttributes();
  public Object[] getValues();
}
