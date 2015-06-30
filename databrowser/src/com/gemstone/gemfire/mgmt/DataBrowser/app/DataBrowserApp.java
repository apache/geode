/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.app;

import java.util.prefs.BackingStoreException;

import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.DSConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.internal.SWTAppAdapter_Controller;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;

/**
 * @author mghosh
 * 
 */
public class DataBrowserApp {

  private AboutAttributes       aa_          = AboutAttributes.getInstance();
  private VersionInfo           vi_          = VersionInfo.getInstance();
  private static DataBrowserApp appInstance_ = new DataBrowserApp();

  private static MainAppWindow          wndMain_     = new MainAppWindow(null);
  private static DataBrowserController  controller_  = new SWTAppAdapter_Controller();


  private static State          state_       = new State.DummyState(5);      // State.create();

  /**
   * 
   */
  private DataBrowserApp() {
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
/*
    for (String strArg : args) {      
      if ("--version".equalsIgnoreCase(strArg)) { 
//        System.out
//            .println("Version: " + "\n" + appInstance_.vi_.toString());
      } // --version
      else if ("--about".equalsIgnoreCase(strArg)) { 
//        System.out
//            .println("About: " + "\n" + appInstance_.aa_.toString()); 
      } // --about
    } // for( String strArg : args )
*/
    
    if(args.length > 0){
      DSConfiguration dsConfig = new DSConfiguration();
      if(args.length == 2 || args.length == 4){  
        dsConfig.setHost(args[0]);
        dsConfig.setPort(Integer.parseInt(args[1]));
      }
      if(args.length == 4){
        dsConfig.setUserName(args[2]);
        dsConfig.setPassword(PasswordUtil.decrypt(args[3]));
      }
      wndMain_.setDsConfig(dsConfig);
    }
    wndMain_.open();
  } // main


  static public DataBrowserApp getInstance() {
    return appInstance_;
  }

  public State getState() {
    return DataBrowserApp.state_;
  }

  public final MainAppWindow getMainWindow() {
    return wndMain_;
  }
  
  public void exit() {
    processShutdown();
    wndMain_.close();
    System.exit(0);
  }
  
  public void openNewCqWindow(GemFireMember member){
    wndMain_.openNewCqWindow(member);
  }
  
  public void processShutdown() {
    getController().disconnect();
    try {
      DataBrowserPreferences.save();
    }
    catch (BackingStoreException e) {
      // TODO do we need to log???
    }
  }

  public final DataBrowserController getController() {
    return controller_;
  }
  
}
