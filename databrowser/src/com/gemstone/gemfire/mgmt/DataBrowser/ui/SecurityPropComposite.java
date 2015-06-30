package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.prefs.BackingStoreException;

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DefaultPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class SecurityPropComposite extends Composite {
  private final static Character ECHO_CHAR            = Character.valueOf( '*' );
  private final static Character CLEAR_ECHO_CHAR      = Character.valueOf( '\0' );

  private final static String GRP_SECURITY            = "Security";
  private static final String LBL_SECPLUGINFQN        = "Security Plugin Jar: ";
  private static final String BTN_BROWSE_SECPLUGINFQN = "Browse...";
  private static final String BUT_NEWSECURITY         = "New...";
  private static final String BUT_DELSECURITY         = "Delete";
  private static final String BUT_DELAllSECURITY      = "Delete All";
  private static final String BUT_IMPORTSECURITY      = "Import...";
  private static final String BUT_HIDDEN              = "hidden";

  // private Composite paneSecurity_ = null;
  private Text                txtSecPluginFQN_        = null;

  private Group               grpSec_;
  private Button              btnBrowseSecPlugin_;
  private Button              btnAddSecProperty_;
  private Button              btnDelSecProperty_;
  private Button              btnDelAllSecProperty_;
  private Button              btnImportSecProperty_;
  private Button              btnHidden_;

  // -- this dialog's data
  private Data                data_                   = new Data();
  private TableViewer         seccurityPropsTable_;

  private ICellModifier       securityPropCellModifier_;

  private CellEditor[] tableTextEditors_;
  private Data securityPropdata_;

  public SecurityPropComposite(Composite parent, int style) {
    super(parent, style);
    createContent();
  }
  
  public SecurityPropComposite(Composite parent, int style, Data data) {
    super(parent, style);
    securityPropdata_ = data;
    createContent();
  }

  private void createContent() {
    GridLayout rwl = new GridLayout(1, false);
    this.setLayout(rwl);

    grpSec_ = new Group(this, SWT.SHADOW_ETCHED_IN);
    grpSec_.setText(SecurityPropComposite.GRP_SECURITY);
    GridLayout lytSec = new GridLayout();
    lytSec.numColumns = 4;
    lytSec.verticalSpacing = 8;
    lytSec.horizontalSpacing = 8;
    grpSec_.setLayout(lytSec);
    grpSec_.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

    GridData gdTxtFlds = new GridData();
    gdTxtFlds.grabExcessHorizontalSpace = true;
    gdTxtFlds.horizontalAlignment = SWT.FILL;
    gdTxtFlds.horizontalSpan = 2;
    gdTxtFlds.widthHint = 300;
    
    GridData gdBtnBrowse = new GridData();
    gdBtnBrowse.horizontalAlignment = SWT.END;
    gdBtnBrowse.horizontalSpan = 1;

    Label lblSecPluginFQN = new Label(grpSec_, SWT.BOLD);
    lblSecPluginFQN.setText(LBL_SECPLUGINFQN);
    txtSecPluginFQN_ = new Text(grpSec_, SWT.BORDER | SWT.READ_ONLY);
    txtSecPluginFQN_.setLayoutData(gdTxtFlds);

    btnBrowseSecPlugin_ = new Button(grpSec_, SWT.NONE);
    btnBrowseSecPlugin_.setText(SecurityPropComposite.BTN_BROWSE_SECPLUGINFQN);
    btnBrowseSecPlugin_.setLayoutData(gdBtnBrowse);
    btnBrowseSecPlugin_.addSelectionListener(new BrowseSecPluginSelLstnr(this));

    btnAddSecProperty_ = new Button(grpSec_, SWT.PUSH);
    btnAddSecProperty_.setText(BUT_NEWSECURITY);
    GridData gd = new GridData();
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.horizontalSpan = 1;
    btnAddSecProperty_.setLayoutData(gd);
    btnAddSecProperty_
        .addSelectionListener(new AddNewSecurityBtnSelLstnr(this));

    createSecPropTableViewer();

    btnDelSecProperty_ = new Button(grpSec_, SWT.PUSH);
    btnDelSecProperty_.setText(BUT_DELSECURITY);
    gd = new GridData();
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.horizontalSpan = 1;
    btnDelSecProperty_.setLayoutData(gd);
    btnDelSecProperty_.setEnabled(false);
    btnDelSecProperty_.addSelectionListener(new DelSecurityBtnSelLstnr());

    btnDelAllSecProperty_ = new Button(grpSec_, SWT.PUSH);
    btnDelAllSecProperty_.setText(BUT_DELAllSECURITY);
    gd = new GridData();
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.horizontalSpan = 1;
    btnDelAllSecProperty_.setLayoutData(gd);
    btnDelAllSecProperty_.addSelectionListener(new DelAllSecurityBtnSelLstnr());


    btnImportSecProperty_ = new Button(grpSec_, SWT.PUSH);
    btnImportSecProperty_.setText(BUT_IMPORTSECURITY);
    gd = new GridData();
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.horizontalSpan = 1;
    btnImportSecProperty_.setLayoutData(gd);
    btnImportSecProperty_
        .addSelectionListener(new ImportSecPropsSelLstnr(this));

    btnHidden_ = new Button(grpSec_, SWT.CHECK);
    btnHidden_.setText(BUT_HIDDEN);
    gd = new GridData();
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.verticalAlignment = SWT.BEGINNING;
    gd.verticalIndent = 10;
    gd.horizontalIndent = 10;
    gd.horizontalSpan = 1;
    btnHidden_.setLayoutData(gd);
    btnHidden_.addSelectionListener(new SelectionListener(){

      public void widgetDefaultSelected(SelectionEvent e) {
        widgetSelected(e);
        
      }

      public void widgetSelected(SelectionEvent e) {
        Button checkButton = (Button) e.widget;
        boolean hidden = checkButton.getSelection();
        List<SecurityProp> securityProperties = data_.secProperties;
        for (int i = 0; i < securityProperties.size(); i++) {
          SecurityProp securityProp = securityProperties.get(i);
          securityProp.setHidden(hidden);
          addNewSecurityProp(securityProp);
        }
      }
      
    });

    setSecurityProp();
  }

  private void createSecPropTableViewer() {
    seccurityPropsTable_ = new TableViewer(grpSec_, SWT.MULTI
        | SWT.FULL_SELECTION | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);

    Table tbl = seccurityPropsTable_.getTable();

    GridData gd = new GridData();
    gd.grabExcessVerticalSpace = true;
    gd.grabExcessHorizontalSpace = true;
    gd.horizontalAlignment = SWT.FILL;
    gd.verticalAlignment = SWT.BEGINNING;
    gd.horizontalSpan = 3;
    gd.verticalSpan = 5;
    gd.heightHint = 100;
    gd.widthHint = 300;

    tbl.setLinesVisible(true);
    tbl.setHeaderVisible(true);
    tbl.setLayoutData(gd);

    final String[] colNames = { SecurityProp.KEY, SecurityProp.VALUE };
    final int[] defColWidths = { 150, 150 };
    final int iNumCols = colNames.length;

    for (int i = 0; i < iNumCols; i++) {
      TableColumn tc = new TableColumn(tbl, SWT.FLAT);
      tc.setResizable(true);
      tc.setText(colNames[i]);
      tc.setWidth(defColWidths[i]);
    }

    seccurityPropsTable_.setContentProvider(new SecurityPropContentProvider(
        this));
    seccurityPropsTable_.setLabelProvider(new SecurityPropLabelProvider(this));
    seccurityPropsTable_.setColumnProperties(new String[]{SecurityProp.KEY, SecurityProp.VALUE});
    tbl.addSelectionListener(new SecurityTableSelLstnr(this));

    // Create the cell editors
    tableTextEditors_ = new CellEditor[iNumCols];
    for (int i = 0; i < iNumCols; i ++)
    {
      TextCellEditor textEditor = new TextCellEditor(seccurityPropsTable_.getTable());
      ((Text) textEditor.getControl()).setTextLimit(Text.LIMIT);
      tableTextEditors_[i] = textEditor;
    }

    // Assign the cell editors to the viewer
    seccurityPropsTable_.setCellEditors(tableTextEditors_);
    ((Text)tableTextEditors_[1].getControl()).addKeyListener(new KeyListener(){

      public void keyPressed(KeyEvent e) {
        Text text= (Text)tableTextEditors_[1].getControl();
        boolean hidden = btnHidden_.getSelection();
        if (hidden)
          text.setEchoChar(ECHO_CHAR);
        else {
          text.setEchoChar(CLEAR_ECHO_CHAR);
        }
      }

      public void keyReleased(KeyEvent e) {
        // TODO Auto-generated method stub

      }

    });

    // set the cell modifier
    securityPropCellModifier_ = new CustomPropCellModifier();
    seccurityPropsTable_.setCellModifier(securityPropCellModifier_);

    seccurityPropsTable_.setInput(data_.secProperties);
  }

  private void setSecurityProp() {
    String securityPlugin =null;
    List<SecurityProp> securityProperties = null;
    boolean hidden = false;
    if(securityPropdata_ == null){
      try {
        securityPlugin = DataBrowserPreferences.getSecurityPlugin();
        securityProperties = DataBrowserPreferences.getSecurityProperties();
        hidden = DataBrowserPreferences.getSecurityPropsHidden();
      }
      catch (BackingStoreException e1) {
        // TODO need to log??
      }
    }else{
      securityPlugin = securityPropdata_.getSecurityPlugin();
      securityProperties = securityPropdata_.getSecurityProperties();
      if(securityProperties.size() > 0)
        hidden = securityProperties.get(0).isHidden();
    }



    btnHidden_.setSelection(hidden);

    setSecurityPlugin(securityPlugin);
    setSecurityProps(securityProperties);

  }
  
  public void setDefaultSecurityProp() {
    String securityPlugin = null;
    List<SecurityProp> securityProperties = null;
    boolean hidden = false;
    
    securityPlugin = DefaultPreferences.DEFAULT_SEURITY_PLUGIN;
    securityProperties = DefaultPreferences.DEFAULT_SEURITY_PROPERTIES;
    hidden = DefaultPreferences.DEFAULT_SEURITY_PROPS_HIDDEN;

    btnHidden_.setSelection(hidden);

    setSecurityPlugin(securityPlugin);
    setSecurityProps(securityProperties);

  }

  private void setSecurityPlugin(String securityPlugin) {
    data_.sPluginJarFQN = securityPlugin;
    txtSecPluginFQN_.setText(securityPlugin);
  }

  private void setSecurityProps(List<SecurityProp> securityProps) {
    //We should remove the existing properties.
    delSecurityProp(null); //This should remove all the existing properties.    
    
    for (SecurityProp prop : securityProps) {
      addNewSecurityProp(prop);
    }
  }

  private void setSecurityProps(String securityProps) throws IOException{
    Properties properties = new Properties();
    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(securityProps);
      properties.load(fileInputStream);
      Set<Entry<Object, Object>> entrySet = properties.entrySet();
      // Set< Entry < Object, Object > > entrySet = properties.entrySet();
      for (Entry<Object, Object> entry : entrySet) {
        Object value = entry.getValue();
        Object key = entry.getKey();
        if (value != null) {
          boolean hidden = btnHidden_.getSelection();
          SecurityProp sProp = new SecurityProp(key.toString(), value
              .toString(), hidden);
          addNewSecurityProp(sProp);
        }
      }
    } catch (IOException ex) {
      throw ex;
    } finally {
      try {
        if (fileInputStream != null)
          fileInputStream.close();
      } catch (IOException e1) {
        LogUtil.warning("Exception while closing stream", e1);
      }
    }
  }

  private void addNewSecurityProp(SecurityProp sProp) {
    if (null != sProp) {
      if (null == data_.secProperties) {
        data_.secProperties = new ArrayList<SecurityProp>();
      }

      if (data_.secProperties.contains(sProp)) {
        seccurityPropsTable_.update(sProp, null);
      }
      else {
        seccurityPropsTable_.add(sProp);
        data_.secProperties.add(sProp);
      }
    }
  }

  private void delSecurityProp(Object[] sProps) {
    Object[] propsList= sProps;
    if(propsList == null)
      propsList = data_.secProperties.toArray();

    for (int i = 0; i < propsList.length; i++) {
      Object sProp = propsList[i];
      if (null != sProp) {
          seccurityPropsTable_.remove(sProp);
          data_.secProperties.remove(sProp);
      }
    }
  }

  public boolean isHidden(){
    return btnHidden_.getSelection();
  }

  static public class SecurityProp {
    static final String KEY = "Key";
    static final String VALUE = "Value";
    static final String SECURITY_PREFIX = "security-";
    private static final String DEFAULT_NAME_STR = "<specify key>";
    private static final String DEFAULT_VALUE_STR = "<specify value>";

    private String key_;

    private String value_;

    private boolean hidden_;

    public SecurityProp(boolean hidden){
      key_ = DEFAULT_NAME_STR;
      value_ = DEFAULT_VALUE_STR;
      hidden_ = hidden;
    }

    public SecurityProp(String key, String value, boolean hidden) {
      key_ = key;
      value_ = value;
      hidden_ = hidden;
    }
    public SecurityProp(String key, String value) {
      key_ = key;
      value_ = value;
      hidden_ = false;
    }

    public String getKey() {
      return key_;
    }

    public String getValue() {
      return value_;
    }

    public void setKey(String key) {
      this.key_ = key;
    }

    public void setValue(String value) {
      this.value_ = value;
    }

    private boolean isHidden() {
      return hidden_;
    }

    private void setHidden(boolean hidden) {
      this.hidden_ = hidden;
    }
  }

  private static class AddNewSecurityBtnSelLstnr extends SelectionAdapter {
    private final SecurityPropComposite prnt_;

    AddNewSecurityBtnSelLstnr(SecurityPropComposite dlg) {
      prnt_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      boolean hidden = prnt_.btnHidden_.getSelection();
      prnt_.addNewSecurityProp((new SecurityProp(hidden)));
    }

  } // class AddNewSecurityBtnSelLstnr

  private class DelSecurityBtnSelLstnr extends SelectionAdapter {

    DelSecurityBtnSelLstnr() {

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      IStructuredSelection selection = (IStructuredSelection)seccurityPropsTable_
          .getSelection();
      Object[] elements = selection.toArray();
      delSecurityProp(elements);
      btnDelSecProperty_.setEnabled(false);
    }
  } // class DelSecurityBtnSelLstnr

  private class DelAllSecurityBtnSelLstnr extends SelectionAdapter {

    DelAllSecurityBtnSelLstnr() {

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      delSecurityProp(null);
      btnDelSecProperty_.setEnabled(false);

    }
  } // class DelSecurityBtnSelLstnr

  private static class SecurityTableSelLstnr extends SelectionAdapter {
    private final SecurityPropComposite prnt_;

    SecurityTableSelLstnr(SecurityPropComposite dlg) {
      prnt_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      Table source = (Table)e.getSource();
      int selectionCount = source.getSelectionCount();
      prnt_.btnDelSecProperty_.setEnabled(selectionCount != 0);
      IStructuredSelection selection = (IStructuredSelection)prnt_.seccurityPropsTable_.getSelection();
      SecurityProp sProp = (SecurityProp)selection.getFirstElement();
      Text text = (Text)prnt_.tableTextEditors_[1].getControl();
      if (sProp.isHidden()
          && !sProp.getValue().equalsIgnoreCase(SecurityProp.DEFAULT_VALUE_STR))
        text.setEchoChar(ECHO_CHAR);
      else
        text.setEchoChar(CLEAR_ECHO_CHAR);
    }
  } // class AddNewSecurityBtnSelLstnr

  private static class BrowseSecPluginSelLstnr extends SelectionAdapter {
    private final SecurityPropComposite parent_;

    BrowseSecPluginSelLstnr(SecurityPropComposite dlg) {
      parent_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      Widget w = e.widget;
      FileDialog dlgBrowse = new FileDialog(w.getDisplay().getActiveShell(),
          SWT.SINGLE);

      String sCurrFQN = null;
      if (null != parent_.data_.sPluginJarFQN
          && 0 != parent_.data_.sPluginJarFQN.length()) {
        sCurrFQN = parent_.data_.sPluginJarFQN;
      }

      dlgBrowse.setFileName(sCurrFQN);
      dlgBrowse.setFilterExtensions(new String[] { "*.jar" });
      String sPluginJar = dlgBrowse.open();
      if (null != sPluginJar) {
        parent_.setSecurityPlugin(sPluginJar);
      }
    }
  } // class BrowseSecPluginSelLstnr

  private static class ImportSecPropsSelLstnr extends SelectionAdapter {
    private final SecurityPropComposite prnt_;

    ImportSecPropsSelLstnr(SecurityPropComposite dlg) {
      prnt_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt
     * .events.SelectionEvent)
     */
    @Override
    public void widgetSelected(SelectionEvent e) {
      super.widgetSelected(e);
      Widget w = e.widget;
      FileDialog dlgBrowse = new FileDialog(w.getDisplay().getActiveShell());
      String sCurrFQN = null;

      dlgBrowse.setFileName(sCurrFQN);
      dlgBrowse.setFilterExtensions(new String[] { "*.properties" });
      String sPropFile = dlgBrowse.open();
      if (null != sPropFile) {
        try {
          prnt_.setSecurityProps(sPropFile);
        }
        catch (IOException ex) {
          // TODO pop up error box
        }
      }
    }
  } // class BrowseSecPropsSelLstnr

  private static class SecurityPropContentProvider implements
      IStructuredContentProvider {
    private final SecurityPropComposite parent_;

    SecurityPropContentProvider(SecurityPropComposite dlg) {
      parent_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IStructuredContentProvider#getElements(java
     * .lang.Object)
     */
    public Object[] getElements(Object inputElement) {
      Object[] res = new Object[0];
      if (inputElement instanceof List) {
        List<?> lst = (List<?>)inputElement;
        if (0 < lst.size()) {
          // MGH - asserting the type
          Object o = lst.get(0);
          if (o instanceof SecurityProp) {
            res = lst.toArray();
          }
        }
      }

      return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.IContentProvider#dispose()
     */
    public void dispose() {
      // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IContentProvider#inputChanged(org.eclipse.jface
     * .viewers.Viewer, java.lang.Object, java.lang.Object)
     */
    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
      // TODO Auto-generated method stub
    }
  } // LctrsContentProvider

  //----------------------------------------------------------------------------
  // ---
  // Label providers
  //----------------------------------------------------------------------------
  // ---

  private static class SecurityPropLabelProvider implements ITableLabelProvider {
    private final SecurityPropComposite parent_;

    SecurityPropLabelProvider(SecurityPropComposite dlg) {
      parent_ = dlg;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.ITableLabelProvider#getColumnImage(java.lang
     * .Object, int)
     */
    public Image getColumnImage(Object element, int columnIndex) {
      // TODO Auto-generated method stub
      return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.ITableLabelProvider#getColumnText(java.lang
     * .Object, int)
     */
    public String getColumnText(Object element, int columnIndex) {
      final SecurityProp sProp;
      if (element instanceof Item) {
        Item itm = (Item)element;
        sProp = (SecurityProp)itm.getData();
      }
      else if (element instanceof SecurityProp) {
        sProp = (SecurityProp)element;
      }
      else {
        sProp = null;
      }

      // TODO MGH - check is there a more elegant way of doing this instead of expecting to
      // act on this result string. Looks like this may have been cut and pasted from the query layer :)
      String res = "Invalid Column Index";
      if (null != sProp) {
        switch (columnIndex) {
          case 0: {
            res = sProp.getKey();
            break;
          }

          case 1: {
            boolean hidden = sProp.isHidden();
            String value = sProp.getValue();
            if (!hidden || (value.equals(SecurityProp.DEFAULT_VALUE_STR)))
              res = value;
            else {
              res = value.replaceAll(".", "*");
            }
            break;
          }

          default: {
            res = "^^^^"; // TODO MGH - what does this string mean?
          }
        }
      }

      return res;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IBaseLabelProvider#addListener(org.eclipse.
     * jface.viewers.ILabelProviderListener)
     */
    public void addListener(ILabelProviderListener listener) {
      // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.IBaseLabelProvider#dispose()
     */
    public void dispose() {
      // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IBaseLabelProvider#isLabelProperty(java.lang
     * .Object, java.lang.String)
     */
    public boolean isLabelProperty(Object element, String property) {
      // TODO Auto-generated method stub
      return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IBaseLabelProvider#removeListener(org.eclipse
     * .jface.viewers.ILabelProviderListener)
     */
    public void removeListener(ILabelProviderListener listener) {

    }
  } // SecurityPropLabelProvider

  public final Data getSecurityPropsData() {
    return data_;
  }

  public final Data populateData() {

    return data_;
  }

  /*
   * Encapsulates the data for this UI window
   */
  public static class Data {
    private String sPluginJarFQN;

    private List<SecurityProp> secProperties;

    Data() {
      sPluginJarFQN = "";
      secProperties = new ArrayList<SecurityProp>();
    }

    public String getSecurityPlugin(){
      return sPluginJarFQN;
    }

    public List<SecurityProp> getSecurityProperties(){
      List<SecurityProp> list = new ArrayList<SecurityProp>();
      for (int i = 0; i < secProperties.size(); i++) {
        SecurityProp securityProp = secProperties.get(i);
        if (!(securityProp.getKey().equals(SecurityProp.DEFAULT_NAME_STR)
            || securityProp.getValue().equals(SecurityProp.DEFAULT_VALUE_STR))) {
          list.add(securityProp);
        }
      }
      return list;
    }
  }

  private class CustomPropCellModifier implements ICellModifier {


    public boolean canModify(Object element, String property) {
      return true;
    }

    public Object getValue(Object element, String property) {
      SecurityProp prop = (SecurityProp)element;
      if (property.equals(SecurityProp.KEY))
        return prop.getKey();
      else
        return prop.getValue();
    }

    public void modify(Object element, String property, Object value) {
      SecurityProp prop;
      if (element instanceof TableItem) {
        TableItem item = (TableItem)element;
        prop = (SecurityProp)item.getData();
      }
      else {
        prop = (SecurityProp)element;
      }

      if (property.equals(SecurityProp.KEY)) {
        String originalKey = prop.getKey();
        String key = ((String)value).trim();
        if ((key.length() == 0) || key.equals(originalKey))
          return;
        if (key.startsWith(SecurityProp.SECURITY_PREFIX))
          prop.setKey(key);
        else
          prop.setKey(SecurityProp.SECURITY_PREFIX + key);
      }
      else {
        String val = ((String)value).trim();
        String originalValue = prop.getValue();
        if (val.length() == 0 || val.equals(originalValue)){
          if(!originalValue.equals(SecurityProp.DEFAULT_VALUE_STR))
            return;
        }

        prop.setValue(val);
        boolean hidden = btnHidden_.getSelection();
        prop.setHidden(hidden);
      }
      // update the actual model
      addNewSecurityProp(prop);
    }

  }

}
