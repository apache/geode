/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import org.eclipse.swt.widgets.Event;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;

/**
 * @author mghosh
 * 
 */
public class Exit extends AbstractDataBrowserAction {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  // SelectionListener lstnrSel_ = new ExitSelectionHandler( this );

  /**
	 * 
	 */
  public Exit() {
    super();
  }

  // *****************************************************************
  //
  // IAction stuff
  //
  // *****************************************************************

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getText()
   */
  @Override
  public String getText() {
    return "Exit";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getDisabledIcon()
   */
  @Override
  public String getDisabledIcon() {
    return iconDisabled;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getEnabledIcon()
   */
  @Override
  public String getEnabledIcon() {
    return iconEnabled;
  }

  
  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getDescription()
   */
  @Override
  public String getDescription() {
    return "Exits the GemFire DataBrowser"; 
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Exit the GemFire DataBrowser application";
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event arg0) {
    DataBrowserApp.getInstance().exit();
  }
}
