/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.window.Window;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Widget;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.SecurityAttributes;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SpecifySecuritiesDlg;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.SecurityProp;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mjha
 * 
 */
public final class SpecifySecurity extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  /**
	 * 
	 */
  public SpecifySecurity() {
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
    return "Security...";
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
    return "Specify the security properties and plugin for the GemFire distributed system";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Security property of DataBrowser to provide credentials at the time of query";
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
     if( null != app ) {
     DataBrowserController ctrlr = app.getController();
     if( null != ctrlr ){
     fRet = ctrlr.hasConnection();
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

    SpecifySecuritiesDlg dlg = new SpecifySecuritiesDlg(shl);
    SecurityPropComposite.Data data = null;

    String errMsg = null;
    boolean fPromptForRetry = false;
    boolean fRetry = false;
    do {
      fRetry = false;
      fPromptForRetry=false;
      dlg.setSecurityData(data);
      int iRetCode = dlg.open();
      if (Window.OK == iRetCode) {
        DataBrowserController cntrlr = DataBrowserApp.getInstance()
            .getController();
        SecurityAttributes sAttrs = new SecurityAttributes();

        data = dlg.getData();
        
        String pluginJarFQN = data.getSecurityPlugin();
        List<SecurityProp> secProperties = data.getSecurityProperties();
        
        if((pluginJarFQN.trim().length() == 0)){
          fPromptForRetry = true;
          errMsg = "No Security plugin provided, \n would you like to specify again ?";
        }
        
        if (true == fPromptForRetry) {
          if (null != errMsg) {
            LogUtil.error(errMsg);
          }
          MessageBox mb = new MessageBox(shl, SWT.YES | SWT.NO);
          mb.setText("Warning");
          if (null == errMsg) {
            errMsg = "No Security plugin provided, \n would you like to specify again ?";
          }

          mb.setMessage(errMsg);
          int iRetry = mb.open();
          fRetry = (SWT.YES == iRetry) ? true : false;
        }
        else {
          Map<String, String> props = new HashMap<String, String>();
          for (int i = 0; i < secProperties.size(); i++) {
            SecurityProp securityProp = secProperties.get(i);
            String key = securityProp.getKey();
            String value = securityProp.getValue();
            props.put(key, value);
          }
          sAttrs.setSecurityPluginPath(pluginJarFQN);
          sAttrs.setSecurityProperties(props);

          cntrlr.setSecurityAttributes(sAttrs);
        }

      } 
    } while (true == fRetry);
  }
}
