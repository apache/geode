/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.resource.ImageDescriptor;

/**
 * @author mghosh
 *
 */
public interface IDataBrowserPrefsPage {
  public String getID();
  public String getLabel();
  public ImageDescriptor getImageDescriptor(); // throws SWTError;
}
