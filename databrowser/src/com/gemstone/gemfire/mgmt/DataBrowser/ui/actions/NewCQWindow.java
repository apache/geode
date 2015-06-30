/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.util.ArrayList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;

/**
 * @author mghosh
 *
 */
public class NewCQWindow extends AbstractDataBrowserAction {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  // SelectionListener lstnrSel_ = new ExitSelectionHandler( this );


  /**
	 *
	 */
  public NewCQWindow() {
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
    return "New Continuous Query";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getDescription()
   */
  @Override
  public String getDescription() {
    return "Creates a new Continuous Query on the selected GemFire member";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Create a new Continuous Query";
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
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event arg0) {
    DataBrowserApp instance = DataBrowserApp.getInstance();

    ArrayList p = new ArrayList<Object>();
    ArrayList r = new ArrayList<Object>();

    p.add(Boolean.FALSE);

    instance.getMainWindow().sendCustomMessage(
        CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC, p, r);

    GemFireMember member = null;
    if (1 == r.size()) {
      Object rv = r.get(0);
      if (rv instanceof GemFireMember) {
        member = (GemFireMember)rv;
      }
    }
    if (member == null) {
      MessageBox mb = new MessageBox(instance.getMainWindow().getShell(), SWT.OK);
      mb.setText("GF DataBrowser: Query Error");
      mb.setMessage("A GemFire member must be selected to execute a Continuous Query. Please select a member from the list.");
      mb.open();
      return;
    }

    if (!member.isNotifyBySubscriptionEnabled()) {
      MessageBox mb = new MessageBox(instance.getMainWindow().getShell(), SWT.OK);
      mb.setText("GF DataBrowser: Query Error");
      mb.setMessage("The GemFire member selected to execute a Continuous Query does not support 'Notify by Subscription'. For more information check out the client/server configuration in the GemFire User's Guide.");
      mb.open();
      return;
    }

    instance.openNewCqWindow(member);
  }
}
