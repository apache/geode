/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Event;

/**
 * @author mghosh
 *
 */
public class TileQueryWindows extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  /**
   *
   */
  public TileQueryWindows() {
    super();
  }

  // **********************************************************
  //
  // IRunnableWithProgress
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.operation.IRunnableWithProgress#run(org.eclipse.core.
   * runtime.IProgressMonitor)
   */
  public void run(IProgressMonitor monitor) throws InvocationTargetException,
      InterruptedException {
  }

  // **********************************************************
  //
  // AbstractDataBrowserAction stuff
  //
  // **********************************************************

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
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getText()
   */
  @Override
  public String getText() {
    return "Tile";
  }

  // **********************************************************
  //
  // IAction stuff
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getDescription()
   */
  @Override
  public String getDescription() {
    return "Arranged the Query Windows in tiled layout.";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Tiles the Query Windows";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    // TODO Auto-generated method stub
    return super.isEnabled();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#run()
   */
  @Override
  public void run() {
    super.run();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.action.Action#runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event event) {
    super.runWithEvent(event);
  }

}
