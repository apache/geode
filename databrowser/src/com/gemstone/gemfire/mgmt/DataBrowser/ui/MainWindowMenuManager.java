/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.action.IContributionItem;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.resource.ImageDescriptor;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator;

/**
 * @author mghosh
 *
 */
public class MainWindowMenuManager extends MenuManager {
/*
  // -- load properties
  static final String                   strPropFile         = "C:\\SVNSandbox\\DataBrowser\\trunk\\src\\com\\gemstone\\gemfire\\mgmt\\DataBrowser\\ui\\MainWindowMenuManager.properties";
  static Properties                     props_              = new Properties();
  static FileInputStream                fisProps_           = null;
  static {
    try {
      MainWindowMenuManager.fisProps_ = new FileInputStream(
          MainWindowMenuManager.strPropFile);
      // props_.loadFromXML( fisProps_ );
    } catch (IOException xptn) {
      System.out.println(xptn.getLocalizedMessage());
      System.out.println(xptn.toString());
      System.exit(-1);
    }
  }
*/
  final static private int              NUM_MENU_BAR_ITEMS  = 4;
  private String[]                      topLevelMenuLabels_ = {
      "&File" , "&Query", "&Options", "&Help" };

//  private String[]                      topLevelMenuLabels_ = {
//      "&File", "&Edit", "&Query", "&Options", "&Window", "&Help" };
  private AbstractDataBrowserAction[][] actionsTypes_       = {
      {
      // -- File menu actions
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ConnectToDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.DisconnectFromDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.SpecifySecurity(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Exit(), },
//      {
//      // -- Edit Menu
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Undo(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Redo(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Cut(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Copy(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Paste(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.SelectAll(), },
      {
      // -- Query menu
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteQuery(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExportQueryResults(),},
      {
//      // -- Options menu actions
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ShowTraceTab(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ShowStatisticsTab(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ShowMessagesTab(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Preferences(), },
//      {
//      // -- Window menu items
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.NewQueryWindow(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.TileQueryWindows(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.CascadeQueryWindows(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.CloseQueryWindow(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.CloseAllQueryWindows(),
//      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
//
//      },
      {
      // -- Help menu items
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.HelpContents(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ActionVisualSeparator(),
      // new org.eclipse.jface.action.Separator(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AboutDataBrowser(), } };
  private MenuManager[]                 menuMgrs_           = null;

  /**
	 *
	 */
  protected MainWindowMenuManager() {
    init();
  }

  /**
   * @param text
   */

  public MainWindowMenuManager(String text) {
    super(text);
    init();
  }

  /**
   * @param text
   * @param id
   */
  protected MainWindowMenuManager(String text, String id) {
    super(text, id);
    init();
  }

  /**
   * @param text
   * @param image
   * @param id
   */
  protected MainWindowMenuManager(String text, ImageDescriptor image, String id) {
    super(text, image, id);
    init();
  }

  // TODO Pass in configuration data specifying the menus
  public MainWindowMenuManager(String text, ImageDescriptor img) {
    super(text, img, text);
    init();
  }

  void init() {
    menuMgrs_ = new MenuManager[MainWindowMenuManager.NUM_MENU_BAR_ITEMS];

    for (int i = 0; i < MainWindowMenuManager.NUM_MENU_BAR_ITEMS; i++) {
      menuMgrs_[i] = new MenuManager(topLevelMenuLabels_[i]);

      menuMgrs_[i].addMenuListener(new IMenuListener(){
        public void menuAboutToShow(IMenuManager manager) {
          IContributionItem[] items = manager.getItems();
          for (int j = 0; j < items.length; j++) {
            items[j].update();
          }
        }
      });

      int iNumActions = actionsTypes_[i].length;
      for (int j = 0; j < iNumActions; j++) {
        AbstractDataBrowserAction adba = actionsTypes_[i][j];
        if (adba instanceof ActionVisualSeparator) {
          menuMgrs_[i].add(new Separator());
        } else {
          menuMgrs_[i].add(adba);
        }
      }

      this.add(menuMgrs_[i]);
    }
  }

}
