/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DefaultPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 *
 */
public class QueryPrefsPage extends PreferencePage implements
    IDataBrowserPrefsPage {

  // -- MGH - fixing a default size. Unable to figure out how to dynamically
  // size (or it is possible?)
  // private static final Point ptDef = new Point( 450, 450 );

  private Composite       parent_   = null;
  private Composite       basePane_ = null;
  private Group           grp1_     = null;
  private ImageDescriptor imgDesc_;
  private Image           img_;
  private Button          enforceLimitOnQuery;
//  private Text            txtTimeout;
  private Text            txtLimit;
//  private Button          btnAutoScrollQueryResults;

  /**
   *
   */
  public QueryPrefsPage() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   */
  public QueryPrefsPage(String title) {
    super(title);
  }

  /**
   * @param title
   * @param image
   */
  public QueryPrefsPage(String title, ImageDescriptor image) {
    super(title, image);
  }



  /* (non-Javadoc)
   * @see org.eclipse.jface.dialogs.DialogPage#dispose()
   */
  @Override
  public void dispose() {
    if( null != img_ ) {
      img_.dispose();
    }

    imgDesc_ = null;
    super.dispose();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.preference.PreferencePage#createContents(org.eclipse.
   * swt.widgets.Composite)
   */
  @Override
  protected Control createContents(Composite prnt) {
    parent_ = prnt;
    basePane_ = new Composite(parent_, SWT.NONE);

    FillLayout lytBase = new FillLayout();
    lytBase.type = SWT.VERTICAL;
    lytBase.marginHeight = 5;
    lytBase.marginWidth = 5;
    basePane_.setLayout(lytBase);

    grp1_ = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    grp1_.setText("Query Execution");

    GridLayout lytGridGrp1 = new GridLayout();
    lytGridGrp1.numColumns = 2;
    lytGridGrp1.verticalSpacing = 20;
    lytGridGrp1.marginHeight = 20;
    lytGridGrp1.makeColumnsEqualWidth = true;
    grp1_.setLayout(lytGridGrp1);

    enforceLimitOnQuery = new Button(grp1_, SWT.CHECK);
    GridData gd = new GridData();
    gd.horizontalSpan = 2;
    enforceLimitOnQuery.setText("Enforce limit on query results");
    enforceLimitOnQuery.setLayoutData(gd);
    enforceLimitOnQuery.setToolTipText("Automatically add a LIMIT clause to the outmost select.");

    Label lblLimit = new Label(grp1_, SWT.BOLD);
    lblLimit.setText( "Limit on result size: ");
    txtLimit = new Text(grp1_, SWT.BORDER);
    txtLimit.setToolTipText("Set the limit on the number of object records returned in the result of the query");
    txtLimit.addModifyListener(new PrefModifyListenerAdapter(this));

    GridData gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    txtLimit.setLayoutData(gridData);

//    Label lblTimeout = new Label(grp1_, SWT.BOLD);
//    lblTimeout.setText("Timeout on Query Execution (ms): ");
//    txtTimeout = new Text(grp1_, SWT.BORDER);
//
//    txtTimeout.setLayoutData(gridData);
//    txtTimeout.setToolTipText("The timeout on the execution of a query in milliseconds");
//
//    btnAutoScrollQueryResults = new Button(grp1_, SWT.CHECK);
//    btnAutoScrollQueryResults.setText("Autoscroll Results: ");
//    btnAutoScrollQueryResults.setSelection(true);
//    btnAutoScrollQueryResults.setToolTipText("Automatically scroll the view displaying the results of the query as records are added");

    enforceLimitOnQuery.addSelectionListener(new SelectionListener(){

      public void widgetDefaultSelected(SelectionEvent e) {
        widgetSelected(e);
      }

      public void widgetSelected(SelectionEvent e) {
        Button but = (Button)e.widget;
        txtLimit.setEnabled(but.getSelection());
        txtLimit.setEditable(but.getSelection());
        
      }

    });

    setPreferences(false);

    return basePane_;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performHelp()
   */
  @Override
  public void performHelp() {
    // TODO Manish - hook to display Help window with relevant information
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performDefaults()
   */
  @Override
  protected void performApply() {
    boolean enforce = enforceLimitOnQuery.getSelection();
    String limitStr = txtLimit.getText();
//    String intervalStr = txtTimeout.getText();
//    boolean autoScroll = btnAutoScrollQueryResults.getSelection();

    long limit;
    long interval;

    try {
      limit = Long.valueOf(limitStr);
    }
    catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Query Limit entered is not valid. \n Please enter a valid (greater than zero) number.");
      mb.open();
      txtLimit.setFocus();
      return ;
    }
    
    if(limit <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Query Limit entered is not valid. \n Please enter a valid (greater than zero) number."); 
      mb.open();
      this.txtLimit.setFocus();
      return;
    }

//    try {
//      interval = Long.valueOf(intervalStr);
//    }
//    catch (NumberFormatException e) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage("Query Execution time out entered is not valid. \n Please enter a valid (greater than zero) number.");
//      mb.open();
//      txtTimeout.setFocus();
//      return;
//    }
//    
//    if(interval <= 0) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage("Query Execution time out entered is not valid. \n Please enter a valid (greater than zero) number."); 
//      mb.open();
//      this.txtTimeout.setFocus();
//      return;
//    }

    DataBrowserPreferences.setEnforceLimitToQuery(enforce);
    if(enforce)
      DataBrowserPreferences.setQueryLimit(limit);
//    DataBrowserPreferences.setQueryExecutionTimeout(interval);
//    DataBrowserPreferences.setResultAutoScroll(autoScroll);

  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performDefaults()
   */
  @Override
  protected void performDefaults() {
    setPreferences(true);
    super.performDefaults();
  }
  
  public boolean performOk() {
    performApply();
    return super.performOk();
  }
  
  @Override
  public boolean isValid() {
   
    String limitStr = txtLimit.getText();
    
    long limit;

    try {
      limit = Long.valueOf(limitStr);
    }
    catch (NumberFormatException e) {
      limit = -1;
    }
    
    if(limit <= 0) {
      setErrorMessage("Query Limit entered is not valid. Please enter a valid (greater than zero) number."); 
      return false;
    }
    
   setErrorMessage(null);  
   return true;
  }

  private void setPreferences(boolean isDefault){
    boolean enforce;
    String value;
    
    if(isDefault) {
      enforce = DefaultPreferences.DEFAULT_ENFORCE_LIMIT_TO_QUERY_RESULT;
      value = String.valueOf(DefaultPreferences.DEFAULT_QUERY_RESULT_LIMIT);   
    
    } else {
      enforce = DataBrowserPreferences.getEnforceLimitToQuery();
      value = String.valueOf(DataBrowserPreferences.getQueryLimit());
    }
 
    enforceLimitOnQuery.setSelection(enforce);
    txtLimit.setEnabled(enforce);
    txtLimit.setText(value);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#setTitle(java.lang.String)
   */
  @Override
  public void setTitle(String title) {
    super.setTitle("Query Options");
  }

  // ----------------------------------------------------------------------------
  // IDataBrowserPrefsPage methods
  // ----------------------------------------------------------------------------

  public String getID() {
    return "Query";
  }

  public String getLabel() {
    return "Query";
  }

  public ImageDescriptor getImageDescriptor() {
    final String fqnImage_ = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/QueryPrefsPageIcon.png";
    InputStream isImage = null;
    try {
      isImage = getClass().getResourceAsStream(fqnImage_);

      if (null != isImage) {
        img_ = new Image(null, isImage);
        imgDesc_ = ImageDescriptor.createFromImage(img_);
      }
    } catch (NullPointerException xptn) {
      // handler for getResourceAsStream
      LogUtil.warning( "NullPointedException in QueryPrefsPage.getImageDescriptor (getResourceAsStream). Continuing...", xptn );
    } catch (SWTException xptn) {
      // handler for org.eclipse.swt.graphics.Image ctor
      // we continue an try to add the other nodes
      LogUtil.warning( "SWTException in QueryPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...", xptn );
    } catch (SWTError err) {
      // Log this (Image ctor could throw this), and rethrow
      LogUtil.error( "SWTError in QueryPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.", err );
      throw err;
    }
    finally {
      if( null != isImage ) {
        try {
          isImage.close();
        } catch (IOException e) {
          LogUtil.warning( "IOException in QueryPrefsPage.getImageDescriptor (isImage.close(..)). Ignoring.", e );
        }
        isImage = null;
      }
    }

    return imgDesc_;
  }

  /**
   * @param args
   *
   *          Test hook
   */
  static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
