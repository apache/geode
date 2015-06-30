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
import com.gemstone.gemfire.mgmt.DataBrowser.app.State;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;

/**
 * @author mghosh
 *
 */
public class DisconnectFromDS extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  static private final String iconEnabled = null;
  static private final String iconDisabled = null;

  /**
	 *
	 */
  public DisconnectFromDS() {
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
    return "Disconnect";
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
    return "Disconnect from the currently connected GemFire distributed system";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Disconnect from the currently connected GemFire distributed system";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.IAction#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    /*
     * State appState = DataBrowserApp.getInstance().getState(); return ((null
     * != appState) && (null != appState.getCurrDS())) ? true : false;
     */
    DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRet = true;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        fRet = ctrlr.hasConnection();
      }
    }

    return fRet;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.action.IAction#runWithEvent(org.eclipse.swt.widgets.Event
   * )
   */
  @Override
  public void runWithEvent(Event arg0) {
    /*
     * State appState = DataBrowserApp.getInstance().getState();
     * DistributedSystem dsCurr = null; if ((null != appState) && (null !=
     * (dsCurr = appState.getCurrDS()))) dsCurr.disconnect();
     */
    DataBrowserApp app = DataBrowserApp.getInstance();
    if (null != app) {
      State st = app.getState();

      DSSnapShot dsss = st.disconnect();
      MainAppWindow wnd = app.getMainWindow();
      if (null != wnd) {
        ArrayList<Object> prms = new ArrayList<Object>();
        ArrayList<Object> res = new ArrayList<Object>();
        DSSnapShot currActiveDS = st.getCurrDS();
        if( null != currActiveDS ) {
          prms.add( st.getCurrDS() );
        }
        wnd.sendCustomMessage(CustomUIMessages.DSTREE_MSG_UPDATE_SNAPSHOT,
            prms, res);
        prms.clear();
        prms.add( dsss );
        wnd.sendCustomMessage(CustomUIMessages.DS_DISCONNECTED, prms, res);
      }
    }
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
  public void run(IProgressMonitor arg0) throws InvocationTargetException,
      InterruptedException {

  }

}
