/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;

/**
 * @author mghosh
 * 
 *         Marker type. Does nothing. The MenuManager checks for this type to
 *         see if a separator should be inserted instead of an Action.
 */
public final class ActionVisualSeparator extends AbstractDataBrowserAction
    implements IRunnableWithProgress {

  /**
   * 
   */
  public ActionVisualSeparator() {
    // TODO Auto-generated constructor stub
  }

  
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getDisabledIcon()
   */
  @Override
  public String getDisabledIcon() {
    // TODO Auto-generated method stub
    return null;
  }



  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getEnabledIcon()
   */
  @Override
  public String getEnabledIcon() {
    // TODO Auto-generated method stub
    return null;
  }



  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.operation.IRunnableWithProgress#run(org.eclipse.core.
   * runtime.IProgressMonitor)
   */
  public void run(IProgressMonitor monitor) throws InvocationTargetException,
      InterruptedException {
    // TODO Auto-generated method stub

  }

}
