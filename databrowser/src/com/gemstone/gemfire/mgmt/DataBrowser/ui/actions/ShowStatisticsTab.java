/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Event;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;

/**
 * @author mghosh
 *
 */
public final class ShowStatisticsTab extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  // -- TODO MGH
  // -- we should remember the checked state from the prior run and
  // -- set this accordingly. Defaults to true
  private boolean fShow_ = true;

  /**
   *
   */
  public ShowStatisticsTab() {
    super();
    setChecked( fShow_ );
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
    return "Show Statistics Tab";
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
    return "Displays the Statistics Tab in the Current Query Pane";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Show the Statistics Tab in the Current Query Pane";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    return super.isEnabled();
  }


  /* (non-Javadoc)
   * @see org.eclipse.jface.action.Action#isChecked()
   */
  @Override
  public boolean isChecked() {
    return super.isChecked();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.action.Action#runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event event) {
    fShow_ = !fShow_;
    setChecked( fShow_ );

    ArrayList< Object > p = new ArrayList< Object >();
    p.add( Boolean.valueOf( fShow_ ));
    ArrayList< Object > r = new ArrayList< Object >();
    DataBrowserApp.getInstance().getMainWindow().sendCustomMessage( CustomUIMessages.QRY_MSG_SHOW_QUERY_STATISTICS_PANE, p, r );
  }
}
