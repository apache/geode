/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.window.Window.IExceptionHandler;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;

/**
 * @author mghosh
 *
 */
public class MainWndGlobalXptnHandler implements IExceptionHandler {

  /**
   * 
   */
  public MainWndGlobalXptnHandler() {
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see org.eclipse.jface.window.Window.IExceptionHandler#handleException(java.lang.Throwable)
   */
  public void handleException(Throwable t) {
    // -- log, cleanup, and die
    DataBrowserApp.getInstance().exit();
  }

}
