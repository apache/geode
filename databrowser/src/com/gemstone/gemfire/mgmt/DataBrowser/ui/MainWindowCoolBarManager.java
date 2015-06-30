/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;


//import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.jface.action.CoolBarManager;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.CoolBar;
import org.eclipse.swt.widgets.CoolItem;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction;

/**
 * @author mghosh
 *
 */
public class MainWindowCoolBarManager extends CoolBarManager {

  /**
	 *
	 */
  public MainWindowCoolBarManager() {
    init();
  }

  /**
   * @param coolBar
   */
  public MainWindowCoolBarManager(CoolBar coolBar) {
    super(coolBar);
    init();
  }

  /**
   * @param style
   */
  public MainWindowCoolBarManager(int style) {
    super(style);
    init();
  }

  protected void init() {
    for (int i = 0; i < actionsTypes_.length; i++) {
      MainWindowToolBarManager tbm = new MainWindowToolBarManager(SWT.FLAT, i);
      this.add(tbm);
    }
  }

  private Menu                          chevronMenu_  = null;

  protected AbstractDataBrowserAction[][] actionsTypes_ = {
      {
      // -- File menu actions
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ConnectToDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.DisconnectFromDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.SpecifySecurity(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Exit() },

      // -- Query menu
      { new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteQuery(),
        new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExportQueryResults(),},

      // -- Help menu
      { new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.HelpContents(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AboutDataBrowser(), }, };

  // ---------
  private static class ChevronClickHandler extends SelectionAdapter {

    MainWindowCoolBarManager manager_;

    ChevronClickHandler(MainWindowCoolBarManager mgr) {
      manager_ = mgr;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent event) {
      //System.out.println("ChevronClickHandler.widgetSelected");
      if (event.detail == SWT.ARROW) {
        CoolItem item = (CoolItem) event.widget;
        CoolBar coolBar = item.getParent();
        Rectangle itemBounds = item.getBounds();
        Point pt = coolBar.toDisplay(new Point(itemBounds.x, itemBounds.y));
        itemBounds.x = pt.x;
        itemBounds.y = pt.y;
        ToolBar bar = (ToolBar) item.getControl();
        ToolItem[] tools = bar.getItems();

        int i = 0;
        while (i < tools.length) {
          Rectangle toolBounds = tools[i].getBounds();
          pt = bar.toDisplay(new Point(toolBounds.x, toolBounds.y));
          toolBounds.x = pt.x;
          toolBounds.y = pt.y;

          /*
           * Figure out the visible portion of the tool by looking at the
           * intersection of the tool bounds with the cool item bounds.
           */
          Rectangle intersection = itemBounds.intersection(toolBounds);

          /*
           * If the tool is not completely within the cool item bounds, then it
           * is partially hidden, and all remaining tools are completely hidden.
           */
          if (!intersection.equals(toolBounds))
            break;

          i++;
        }

        /* Create a menu with items for each of the completely hidden buttons. */
        if (null != manager_.chevronMenu_)
          manager_.chevronMenu_.dispose();

        manager_.chevronMenu_ = new Menu(coolBar);
        for (int j = i; j < tools.length; j++) {
          MenuItem menuItem = new MenuItem(manager_.chevronMenu_, SWT.PUSH);
          menuItem.setText(tools[j].getText());
        }

        /* Drop down the menu below the chevron, with the left edges aligned. */
        pt = coolBar.toDisplay(new Point(event.x, event.y));
        manager_.chevronMenu_.setLocation(pt.x, pt.y);
        manager_.chevronMenu_.setVisible(true);
      }
      // super.widgetSelected(event);
    }

  } // class ChevronClickHandler

}
