/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.util.ArrayList;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.window.Window;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Widget;

import com.gemstone.gemfire.LicenseException;
import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.app.State;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.DSConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.JMXOperationFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.VersionMismatchException;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.ConnectToDSDlg;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 *
 */
public final class ConnectToDS extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  static private final String iconEnabled = null;
  static private final String iconDisabled = null;

  /**
   *
   **/
  public ConnectToDS() {
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
  public void run(IProgressMonitor arg0) throws InvocationTargetException,
      InterruptedException {
  }

  // **********************************************************
  //
  // AbstractDataBrowserAction stuff
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   *
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getText()
   */
  @Override
  public String getText() {
    return "Connect to GemFire...";
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
    // TODO Auto-generated method stub
    return iconEnabled;
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
    return "Connect to a GemFire distributed system";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Connects to the specified GemFire distributed system";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRet = true;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        fRet = false == ctrlr.hasConnection();
      }
    }

    return fRet;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.action.Action#runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event event) {
    Shell shl = null;
    Display dsply = event.display;
    if (null != dsply) {
      shl = dsply.getActiveShell();
    }

    if (null == shl) {
      Widget w = event.widget;
      if (null != w) {
        shl = w.getDisplay().getActiveShell();
      }
    }

    ConnectToDSDlg dlg = new ConnectToDSDlg(shl);
    ConnectToDSDlg.Data data = dlg.getData();

    boolean fRetry = false;
    String errMsg = null;
    Throwable ex = null;
    boolean fPromptForRetry = false;

    do {
      int iRetCode = dlg.open();
      fRetry = false;
      if (Window.OK == iRetCode) {
        // TODO MGH - shl could be null here?
        Shell shlDlg = dlg.getShell();
        Control cursorControl = null;
        if (null != shlDlg) {
          Display dsplyDlg = shlDlg.getDisplay();
          if (null != dsply) {
            cursorControl = dsplyDlg.getCursorControl();
            if (cursorControl != null) {
              cursorControl.setCursor(new Cursor(dsply, SWT.CURSOR_WAIT));
            }
          }
        }

        DataBrowserController cntrlr = DataBrowserApp.getInstance()
            .getController();
        DSConfiguration cfg = new DSConfiguration();

        data = dlg.getData();
        cfg.setHost(data.host);
        cfg.setPort(data.port);
        cfg.setUserName(data.userName);
        cfg.setPassword(data.password);
        boolean fConnected = false;
        try {
          if (true == (fConnected = cntrlr.connect(cfg))) {
            DSSnapShot dsss = cntrlr.getDSSnapShot();

            DataBrowserApp app = DataBrowserApp.getInstance();
            if (null != app) {
              State st = app.getState();
              st.addDS(dsss);
              st.switchToDS(dsss);
              MainAppWindow wnd = app.getMainWindow();
              if (null != wnd) {
                ArrayList<Object> prms = new ArrayList<Object>();
                ArrayList<Object> res = new ArrayList<Object>();
                prms.add(dsss);
                wnd.sendCustomMessage(
                    CustomUIMessages.DSTREE_MSG_UPDATE_SNAPSHOT, prms, res);
                wnd.sendCustomMessage(CustomUIMessages.DS_CONNECTED, prms, res);
              }
            }
            dsss = null;
          }
        }
        // TODO MGH - need to clean this up as this exception is not thrown
        /*
         * catch (InvalidConfigurationException xptn) { errMsg =
         * "GemFire DataBrowser - Connection Error";
         * fPromptForRetry = true; }
         */
        catch (ConnectionFailureException xptn) {
          errMsg = "An error occurred when attempting to connect to the GemFire Distributed System";
          ex = xptn;
          fPromptForRetry = xptn.isRetryAllowed();
        } finally {
          if (cursorControl != null)
            if (!cursorControl.isDisposed())
              cursorControl.setCursor(new Cursor(dsply, SWT.CURSOR_ARROW));
        }

        if (false == fConnected) {
          // String[] hs = cfg.get;
          // int[] ps = cfg.getPortList();
          // StringBuffer dsInfo = new StringBuffer();
          // if( true == cfg.isMcastEnabled() ) {
          //              dsInfo.append( "Using MultiCast >> " );
          //              dsInfo.append( hs[0] + ":" + ps[0] );
          // }
          // else {
          // int itms = Math.min( hs.length, ps.length );
          //
          //              dsInfo.append( "Using Locators >>" );
          // for( int i = 0; i < itms; i++ ) {
          //                dsInfo.append( hs[0]+ ":" + ps[0] + "\n" );
          // }
          // }
          //
          //            errMsg = "Failed to connect to the GemFire Distributed System at " + dsInfo.toString();
          // fPromptForRetry = true;
        }

        if (ex != null) {
          LogUtil.error(errMsg, ex);
          MessageBox mb = new MessageBox(shl, SWT.OK);
          if (fPromptForRetry) {
            mb = new MessageBox(shl, SWT.OK | SWT.CANCEL);
          } else {
            mb = new MessageBox(shl, SWT.OK);
          }
          mb.setText( "GemFire DataBrowser - Connection Error" );

          StringBuilder builder = new StringBuilder();

          if (ex instanceof VersionMismatchException) {
            builder.append("Connection attempt to the GemFire Distributed System failed. Please verify that the DataBrowser is using same version of GemFire as the system you are connecting." );
          } else  if(ex.getCause() instanceof SecurityException){
              builder.append("Security is enabled on the JMX Manager. Valid credentials required");
          } else {
              builder.append(getExceptionDescription(ex, cfg, fPromptForRetry));
          }
          
          mb.setMessage(builder.toString());
          int iRetry = mb.open();
          fRetry = fPromptForRetry && (SWT.OK == iRetry);
          fPromptForRetry = false;
          ex = null;
        }
      } // if( Window.OK = iRetCode )
    } while (true == fRetry);

  }
  
  public static boolean connect(DSConfiguration cfg){
    DataBrowserController cntrlr = DataBrowserApp.getInstance()
        .getController();
    try {
      if (cntrlr.connect(cfg)) {
        DSSnapShot dsss = cntrlr.getDSSnapShot();

        DataBrowserApp app = DataBrowserApp.getInstance();
        if (null != app) {
          State st = app.getState();
          st.addDS(dsss);
          st.switchToDS(dsss);
          MainAppWindow wnd = app.getMainWindow();
          if (null != wnd) {
            ArrayList<Object> prms = new ArrayList<Object>();
            ArrayList<Object> res = new ArrayList<Object>();
            prms.add(dsss);
            wnd.sendCustomMessage(
                CustomUIMessages.DSTREE_MSG_UPDATE_SNAPSHOT, prms, res);
            wnd.sendCustomMessage(CustomUIMessages.DS_CONNECTED, prms, res);
          }
        }
        dsss = null;
        return true;
      }
    }  catch (ConnectionFailureException e){
      //no shell available not sure how to prompt user
      LogUtil.error( "An error occurred when attempting to connect to the GemFire Distributed System", e);
    }
    return false;
  }
  
  private String getExceptionDescription(Throwable ex, DSConfiguration cfg, boolean promptForRetry) {
    StringBuffer buffer = new StringBuffer();
    Throwable cause = ex.getCause();
    
    while(cause != null) {
      if(cause instanceof ConnectException) {
        buffer.append("Following error occurred when attempting to connect to the GemFire Distributed System.");
        buffer.append("\n\nFailed to connect to the GemFire Locator/JMX Manager at ["+cfg.getHost()+":"+cfg.getPort()+"].");
        buffer.append(" Please verify if the GemFire Locator/JMX Manager is running.");
        if (promptForRetry) {
          buffer.append("\n\nWould you like to reattempt connecting?");
        }
        return buffer.toString();        
      
      } else if(cause instanceof LicenseException) {
        buffer.append("Following error occurred when attempting to connect to the GemFire Distributed System.");
        buffer.append("\n\nLicense file \"gemfireLicense.zip\" does not exist in the current directory nor in the product directory.");
        buffer.append("\nPlease provide a valid GemFire license in order to connect to the GemFire distributed system.");
        if (promptForRetry) {
          buffer.append("\n\nWould you like to reattempt connecting with new settings?");
        }
        return buffer.toString();
      
      } else if(cause instanceof JMXOperationFailureException) {
        buffer.append("Following error occurred when attempting to connect to the GemFire Distributed System.");
        buffer.append("\n\n");
        buffer.append(cause.getMessage());
        if (promptForRetry) {
          buffer.append("\n\nWould you like to reattempt connecting?");
        }
        return buffer.toString();        
      }
      
      cause = cause.getCause();      
    }
    
    buffer.append("Following error occurred when attempting to connect to the GemFire Distributed System.");
    buffer.append("\n\n");
    buffer.append(ex.getMessage());
    if (promptForRetry) {
      buffer.append("\n\nWould you like to reattempt connecting with new settings?");
    }
    
    return buffer.toString();
  }
   
}
