/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Widget;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.AboutDlg;

/**
 * @author mghosh
 * 
 */
public class AboutDataBrowser extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String ABOUT_DATABROWSER = "/resources/aboutvFabric_GFDataBrowser.txt";
  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  /**
   * 
   */
  public AboutDataBrowser() {
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getText()
   */
  @Override
  public String getText() {
    return "About GemFire DataBrowser...";
  }

  
  private String getAboutMessageText() {
    StringBuilder about = new StringBuilder();
    InputStream is = AboutDataBrowser.class.getResourceAsStream(ABOUT_DATABROWSER);
    if( is == null ) {
      about.append("Unable to retrieve specified resource.");
    } else {
       BufferedReader input = null;
       try {
         input = new BufferedReader(new InputStreamReader(is));
         String line = null;
         while (( line = input.readLine()) != null){
           about.append(line);
           about.append(System.getProperty("line.separator"));
         }
       } catch( IOException ioe ) {
         about.setLength(0); //clear text
         about.append("Unable to retrieve specified resource.");
         about.append(System.getProperty("line.separator"));
         about.append(ioe.toString());
       } finally { 
         if(input != null) {
           //is will be closed when it's wrapping BufferedReader is closed.
           try {input.close();} catch(IOException ignore) {}
         } else {
           //is cannot be null
           try {is.close();} catch(IOException ignore) {}
         }
       } 
    }
    return about.toString();
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
    return "Displays the 'About DataBrowser' window";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Show the 'About DataBrowser' window";
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

    final String text = getAboutMessageText(); 
    AboutDlg dlg = new AboutDlg(shl, text);
    dlg.setTitle("About GemFire DataBrowser"); 
    dlg.setHelpAvailable(false);
    dlg.open();
  }

}
