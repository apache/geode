/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.io.File;
import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Event;

import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 * 
 */
public final class HelpContents extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String iconEnabled  = null;
  private static final String iconDisabled = null;

  private static final String fileNameUM_  = "index.html";
  private static String fqnUM_ = null;
//  private static URI          userManualURI_;

  private static boolean      fEnabled_    = true;

  static {
    try {
      Class<?> clz = Class
          .forName("com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.HelpContents");
      String umLoc = clz.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
      final String uncSeparator = "/";

      // Remove trailing edge "/"
      int lastIndex = umLoc.lastIndexOf(uncSeparator);
      if (lastIndex == umLoc.length() - 1) {
        umLoc = umLoc.substring(0, lastIndex);
        lastIndex = umLoc.lastIndexOf(uncSeparator);
      }

      umLoc = umLoc.substring(0, lastIndex);

      // Go to jarFile/../..
      lastIndex = umLoc.lastIndexOf(uncSeparator);
      umLoc = umLoc.substring(0, lastIndex);

      HelpContents.fqnUM_ = umLoc + uncSeparator + "docs" + uncSeparator
          + HelpContents.fileNameUM_;
      File file = new File(HelpContents.fqnUM_);
      if (false == file.exists()) {
        HelpContents.fEnabled_ = false;
        LogUtil.info("DataBrowser User Manual " + file.getPath() + " does not exist.");
      } else {
        LogUtil.info("DataBrowser User Manual path is " + file.getPath());
        //HelpContents.userManualURI_ = file.toURI();
      }
    } catch (Throwable t) {
      LogUtil.error("DataBrowser UserManual ( " + HelpContents.fqnUM_
          + " ) could not be loaded.", t);
      HelpContents.fEnabled_ = false;
    }
  }

  /**
   *
   */
  public HelpContents() {
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
    return "Help...";
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getDisabledIcon()
   */
  @Override
  public String getDisabledIcon() {
    return iconDisabled;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getEnabledIcon()
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
    return "Displays DataBrowser User Manual";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Show the User Manual";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    return HelpContents.fEnabled_;
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
    /*
    if (Desktop.isDesktopSupported()) {
      try {
        Desktop desktop = Desktop.getDesktop();
        desktop.browse(HelpContents.userManualURI_);
      } catch (IOException e) {
        LogUtil.error( "Could not launch user manual for viewing.",  e );
      }
    }
    */
   
    Program.launch( "file://" + HelpContents.fqnUM_ );
  }
}
