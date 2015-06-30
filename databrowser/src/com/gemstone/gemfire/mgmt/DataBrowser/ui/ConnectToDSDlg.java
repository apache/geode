/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.LinkedList;

import org.eclipse.jface.dialogs.TrayDialog;
import org.eclipse.jface.window.IShellProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.HelpEvent;
import org.eclipse.swt.events.HelpListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.DataValidator;

/**
 * @author mghosh
 *
 */
public class ConnectToDSDlg extends TrayDialog {

  private final static String DIALOG_TITLE = "Connect to GemFire Distributed System";
  private final static String GRP_JMX      = "Connect to Gemfire Locator/JMX Manager";
  private static final String LBL_HOST     = "Host: ";
  private static final String LBL_PORT     = "Port: ";
  private static final String LBL_USER     = "Username: ";
  private static final String LBL_PSSWD     = "Password: ";

  private Composite           parent_      = null;
  private Group               grpJmx_;

  // -- this dialog's data
  private Data                data_        = new Data();
  private Combo                txtHost_;
  private Combo                txtPort_;
  private Text                txtUser_;
  private Text                txtPsswd_;

  private LinkedList<String> enteredHosts = new LinkedList<String>();

  private LinkedList<String> enteredPorts = new LinkedList<String>();

  /**
   * @param shell
   */
  public ConnectToDSDlg(Shell shell) {
    super(shell);
  }

  /**
   * @param parentShell
   */
  public ConnectToDSDlg(IShellProvider parentShell) {
    super(parentShell);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.dialogs.Dialog#createContents(org.eclipse.swt.widgets
   * .Composite)
   */
  @Override
  protected Control createContents(Composite prnt) {
    parent_ = prnt;
    parent_.getShell().setText(ConnectToDSDlg.DIALOG_TITLE);
    GridLayout rwl = new GridLayout(1, false);
    parent_.setLayout(rwl);

    grpJmx_ = new Group(parent_, SWT.SHADOW_ETCHED_IN);
    grpJmx_.setText(GRP_JMX);
    grpJmx_.setLayoutData(new GridData(SWT.FILL, SWT.BEGINNING, true, true));
    GridLayout lytGrpDisc = new GridLayout();
    lytGrpDisc.numColumns = 6;
    lytGrpDisc.marginRight = 10;
    lytGrpDisc.makeColumnsEqualWidth = true;
    grpJmx_.setLayout(lytGrpDisc);

    Label lblMCastAddr = new Label(grpJmx_, SWT.BOLD);
    lblMCastAddr.setText(LBL_HOST);
    txtHost_ = new Combo(grpJmx_, SWT.DROP_DOWN | SWT.BORDER);
    GridData gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    gridData.horizontalSpan = 5;
    txtHost_.setLayoutData(gridData);
    txtHost_.addListener(SWT.FocusIn, getHostListener());
    if (!DataBrowserPreferences.getConnectionMRUHosts().isEmpty())
      txtHost_.setText(DataBrowserPreferences.getConnectionMRUHosts().getFirst());

    Label lblMCastAddr1 = new Label(grpJmx_, SWT.BOLD);
    lblMCastAddr1.setText(LBL_PORT);
    txtPort_ = new Combo(grpJmx_, SWT.DROP_DOWN | SWT.BORDER);
    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    gridData.horizontalSpan = 5;
    txtPort_.setLayoutData(gridData);
    txtPort_.addListener(SWT.FocusIn, getPortListener());
    if (!DataBrowserPreferences.getConnectionMRUPorts().isEmpty())
      txtPort_.setText(DataBrowserPreferences.getConnectionMRUPorts().getFirst());

    enteredHosts = DataBrowserPreferences.getConnectionMRUHosts();
    enteredPorts = DataBrowserPreferences.getConnectionMRUPorts();

    Label lblUserName = new Label(grpJmx_, SWT.BOLD);
    lblUserName.setText(LBL_USER);
    txtUser_ = new Text(grpJmx_,  SWT.BORDER);
    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    gridData.horizontalSpan = 5;
    txtUser_.setLayoutData(gridData);
    
  
    Label lblPassword = new Label(grpJmx_, SWT.BOLD);
    lblPassword.setText(LBL_PSSWD);
    txtPsswd_ = new Text(grpJmx_, SWT.BORDER);
    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    gridData.horizontalSpan = 5;
    txtPsswd_.setLayoutData(gridData);
    txtPsswd_.setEchoChar('*');
    
    Control ctrlRet = super.createContents(parent_);
//    parent_.addHelpListener( new C2DSHelpLstnr( this ));

    return ctrlRet;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.dialogs.TrayDialog#isHelpAvailable()
   */
  @Override
  public boolean isHelpAvailable() {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.dialogs.TrayDialog#createHelpControl(org.eclipse.swt.
   * widgets.Composite)
   */
  // @Override
  // protected Control createHelpControl(Composite parent) {
  // Control ctrlHelp = super.createHelpControl(parent);
  // getParentShell().addHelpListener(new C2DSHelpLstnr(this));
  // return ctrlHelp;
  // }
  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.dialogs.Dialog#initializeBounds()
   */
  @Override
  protected void initializeBounds() {
    super.initializeBounds();

    //
    // MGH There seems to be bug in the docs for getBounds() and setBounds().
    // getBounds() seems to return 'absolute' coordinates. Hence, setting x, y
    // in
    // setBounds() to those returned by getBounds() and changing the width to
    // widen the control results in the control being offset by 'x' pixels.
    //

    // TODO MGH - perhaps layout info can be used to get the margin width.
    // Using default 5 px for now
    // -- set bounds of the pane containing the locator and multi-case data
    // panes
    Rectangle rcAncestorClntArea = grpJmx_.getClientArea();
    Rectangle rcCurrElemBounds = grpJmx_.getBounds();
    grpJmx_.setBounds(5, rcCurrElemBounds.y, rcAncestorClntArea.width - 10,
        rcCurrElemBounds.height);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.dialogs.Dialog#okPressed()
   */
  @Override
  protected void okPressed() {
    // We keep the data to restore it if 'retry' is attempted.
    // -- Get security stuff
    data_.host = this.txtHost_.getText().trim();
    String sPort = this.txtPort_.getText().trim();
    data_.userName = this.txtUser_.getText().trim();
    data_.password = this.txtPsswd_.getText().trim();
    boolean fErr = false;
    if (this.data_.host.trim().length() == 0) {
      String errMsg = "";
      Shell shl = getShell();
      MessageBox mb = new MessageBox(shl, SWT.OK);
      mb.setText("GemFire DataBrowser - Connection Error");
      errMsg = "No host entered";
      mb.setMessage(errMsg);
      mb.open();

      fErr = true;
    }

    boolean isValidPort = DataValidator.isValidTCPPort(sPort);

    if(!fErr ) {
      if( false == isValidPort ) {
        String errMsg = "";
        Shell shl = getShell();
        MessageBox mb = new MessageBox(shl, SWT.OK);
        mb.setText("GemFire DataBrowser Error");
        errMsg = "Port entered is not valid";
        if( 0 != sPort.trim().length())
          errMsg += " [" + sPort + "]";
        mb.setMessage(errMsg);
        mb.open();
        fErr = true;
      } else {
        data_.port = Integer.valueOf(sPort).intValue();
      }
    }

    if (false == fErr) {
      adjustConnectionMRUHostList(data_.host);
      adjustConnectionMRUPortList(String.valueOf(sPort));
      super.okPressed();
    }
  }

  // -----------------------------------------------------------------
  // Listener for help control
  // -----------------------------------------------------------------
  private static class C2DSHelpLstnr implements HelpListener {
    private final ConnectToDSDlg parent_;

    C2DSHelpLstnr(ConnectToDSDlg dlg) {
      parent_ = dlg;
    }

    public void helpRequested(HelpEvent e) {
      parent_.showHelp();
    }
  } // C2DSHelpLstnr

  public void showHelp() {
    // TODO MGH - hook to display Help window with relevant information
    MessageBox mb = new MessageBox(getShell());
    mb.setText("GemFire DataBrowser");
    mb.setMessage("Would show help for Connecting to a DS here!!");
    mb.open();
  }

  protected void adjustConnectionMRUHostList(String host) {
    if(DataBrowserPreferences.getConnectionMRUHListLimit() == 0) {
      return;
    }

    //First clear the position.
    if(!enteredHosts.remove(host)) {
      //If the list is full, clear the last entry.
      if(enteredHosts.size() == DataBrowserPreferences.getConnectionMRUHListLimit()) {
        enteredHosts.removeLast();
      }
    }

    //Add the entry to the top.
    enteredHosts.addFirst(host);

    DataBrowserPreferences.setConnectionMRUHosts(enteredHosts);
  }

  protected void adjustConnectionMRUPortList(String port) {
    if(DataBrowserPreferences.getConnectionMRUHListLimit() == 0) {
      return;
    }

    //First clear the position.
    if(!enteredPorts.remove(port)) {
      //If the list is full, clear the last entry.
      if(enteredPorts.size() == DataBrowserPreferences.getConnectionMRUHListLimit()) {
        enteredPorts.removeLast();
      }
    }

    //Add the entry to the top.
    enteredPorts.addFirst(port);

    DataBrowserPreferences.setConnectionMRUPorts(enteredPorts);
  }

  public final ConnectToDSDlg.Data getData() {
    return data_;
  }

  public void setData(ConnectToDSDlg.Data d) {
    data_ = d;
  }

  protected Listener getHostListener() {
    return new Listener() {
      /* currently entered value in the combo */
      private String currentValue;

      public void handleEvent(Event event) {
        Combo widget = (Combo)event.widget;
        currentValue = widget.getText();
        widget.removeAll();
        if (currentValue != null && !currentValue.equals("")) {
          widget.add(currentValue);
          widget.setText(currentValue);
        }
        LinkedList<String> hosts = DataBrowserPreferences.getConnectionMRUHosts();
        for(String host : hosts) {
          if(!host.equalsIgnoreCase(currentValue)) {
            widget.add(host);
          }
        }
      }
    };
  }

  protected Listener getPortListener() {
    return new Listener() {
      /* currently entered value in the combo */
      private String currentValue;

      public void handleEvent(Event event) {

        Combo widget = (Combo)event.widget;
        currentValue = widget.getText();
        widget.removeAll();
        if (currentValue != null && !currentValue.equals("")) {
          widget.add(currentValue);
          widget.setText(currentValue);
        }
        LinkedList<String> ports = DataBrowserPreferences.getConnectionMRUPorts();
        for(String port : ports) {
          if(!port.equalsIgnoreCase(currentValue)) {
            widget.add(port);
          }
        }
      }
    };
  }

  /*
   * Encapsulates the data for this UI window
   */
  public static class Data {
    public String host;
    public int    port;
    public String userName;
    public String password;

    Data() {
      host = "";
      port = 0;
      userName = "";
      password = "";
    }
  }

  public static void main(String[] args) {
    ConnectToDSDlg connectToDSDlg = new ConnectToDSDlg((Shell) null);
    connectToDSDlg.open();
  }

}
