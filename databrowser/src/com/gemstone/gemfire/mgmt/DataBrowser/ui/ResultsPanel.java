/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.viewers.ColumnPixelData;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ILazyContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.ControlListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.TableColumn;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.model.QueryResultsInput;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.tree.ExploreAction;

/**
 * @author mghosh
 *
 */
public class ResultsPanel extends Composite {
  private static final int   WIDTH                = 100;

  private List<TableViewer>  tblResults_          = null;
  private CTabFolder         resultsContainer_;

  /**
   * @param parent
   * @param style
   */
  public ResultsPanel(Composite parent, int style) {
    super(parent, style);
    this.setBackground(this.getDisplay().getSystemColor(SWT.COLOR_GREEN));

    GridLayout fl = new GridLayout();
    this.setLayout(fl);
    resultsContainer_ = new CTabFolder(this, SWT.NONE);
    resultsContainer_.setBorderVisible(true);
    // Display display = this.resultsPane_.getShell().getDisplay();
    resultsContainer_
        .addControlListener(new ResultsTabFolderResizeHandler(this));

    resultsContainer_.setSimple(false);
    resultsContainer_.setUnselectedCloseVisible(true);
    resultsContainer_.setUnselectedImageVisible(true);

    resultsContainer_
        .setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    addDummyTabs();

    this.layout();
    this.pack();

  }

  private void addDummyTabs() {
    resultsContainer_.setTabHeight(0);
    int iStyle = SWT.NONE;
    iStyle |= SWT.BORDER;
    CTabItem ti = new CTabItem(resultsContainer_, iStyle, 0);
    ti.setControl(getDummyTable());
    resultsContainer_.setSelection(0);
  }

  private Composite getDummyTable() {
    Composite tableComp = new Composite(resultsContainer_, SWT.NONE);
    GridLayout layout = new GridLayout();
    tableComp.setLayout(layout);
    tableComp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    TableViewer viewer = new TableViewer(tableComp, SWT.SINGLE
        | SWT.FULL_SELECTION | SWT.H_SCROLL | SWT.V_SCROLL | SWT.VIRTUAL);

    TableLayout lytTable = new TableLayout();

    for (int i = 0; i < 2; i++) {
      lytTable.addColumnData(new ColumnPixelData(WIDTH, true));
    }

    viewer.getTable().setLayout(lytTable);
    viewer.getTable().setHeaderVisible(true);
    viewer.getTable().setLinesVisible(true);

    GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
    viewer.getTable().setLayoutData(gd);

    String colName = "Key";
    TableColumn col = new TableColumn(viewer.getTable(), SWT.NONE, 0);
    col.setText(colName);
    col.setResizable(true);

    colName = "Value";
    col = new TableColumn(viewer.getTable(), SWT.NONE, 1);
    col.setText(colName);
    col.setResizable(true);

    return tableComp;
  }


  private void createTable(Composite parent,QueryResult results, IntrospectionResult result) {
    TableViewer viewer = new TableViewer(parent, SWT.SINGLE
        | SWT.FULL_SELECTION | SWT.H_SCROLL | SWT.V_SCROLL | SWT.VIRTUAL);

    TableLayout lytTable = new TableLayout();
    int columnCount = result.getColumnCount();

    for (int i = 0; i < columnCount; i++) {
      lytTable.addColumnData(new ColumnPixelData(WIDTH, true));
    }

    viewer.getTable().setLayout(lytTable);
    viewer.getTable().setHeaderVisible(true);
    viewer.getTable().setLinesVisible(true);

    GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
    viewer.getTable().setLayoutData(gd);
    for (int i = 0; i < columnCount; i++) {
      try {
        String colName = result.getColumnName(i);
        TableColumn col = new TableColumn(viewer.getTable(), SWT.NONE, i);
        col.setText(colName);
        col.setResizable(true);
      }
      catch (ColumnNotFoundException e) {
        continue;
      }
    }

    QueryResultsInput queryResultsInput = new QueryResultsInput(viewer,results, result);
    int size = queryResultsInput.getSize();
    viewer.setItemCount(size);
    viewer.setContentProvider(new ResultTableContentProvider(viewer));

    viewer.setLabelProvider(new ResultTableLabelProvider(viewer));
    viewer.setInput(queryResultsInput);


    ExploreAction exploreAction = new ExploreAction(this.getShell(), viewer);
    MenuManager manager = new MenuManager();
    viewer.getTable().setMenu(manager.createContextMenu(viewer.getTable()));
    manager.add(exploreAction);

    viewer.addDoubleClickListener(exploreAction);
    if (tblResults_ == null)
      tblResults_ = new ArrayList<TableViewer>();

    tblResults_.add(viewer);

    // this.tblResults_.getTable().addControlListener( new
    // PaneResizeHandler(
    // this ));
  }



  private Composite getTablePane(QueryResult results, IntrospectionResult result){
    Composite tableComp = new Composite(resultsContainer_, SWT.NONE);
    GridLayout layout = new GridLayout();
    tableComp.setLayout(layout);
    tableComp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    createTable(tableComp, results, result);
    return tableComp;
  }


  private void addTabs(QueryResult results) {
    resultsContainer_.setTabHeight(SWT.DEFAULT);
    IntrospectionResult[] introspectionResults = results.getIntrospectionResult();
    for (int i = 0; i < introspectionResults.length; i++) {
      int iStyle = SWT.NONE;
      iStyle |= SWT.BORDER;
      CTabItem ti = new CTabItem(resultsContainer_, iStyle, i);
      ti.setText(introspectionResults[i].getJavaTypeName());
      ti.setControl(getTablePane(results, introspectionResults[i]));
    }
    resultsContainer_.setSelection(0);
  }

  public void showResults(Object oResults) {
    if (!(oResults instanceof QueryResult))
      return;// only QueryResult is expected

    disposeOldResults();

    QueryResult results = (QueryResult)oResults;

    if(results.isEmpty()) {
      MessageBox mb = new MessageBox(getShell(), SWT.OK);
      mb.setText("Query Result");
      mb.setMessage("Query result is empty.");
      mb.open();
    } else {
      addTabs(results);
    }
  }

  public void handleEventForConnection() {
    if(tblResults_ != null){
     disposeOldResults();
     addDummyTabs();
    }
  }

  void disposeOldResults() {
    CTabItem[] items = resultsContainer_.getItems();
    for (int i = 0; i < items.length; i++) {
      items[i].getControl().dispose();
      items[i].dispose();
    }
    if (tblResults_ != null && !tblResults_.isEmpty()){
    tblResults_.clear();
    tblResults_ = null;
    }
  }

  private static class ResultTableContentProvider implements
      ILazyContentProvider {

    private TableViewer viewer_;

    ResultTableContentProvider(TableViewer viewer) {
      viewer_ = viewer;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.ILazyContentProvider#getElements(java
     * .lang.Object)
     */
    public void updateElement(int index) {
      QueryResultsInput input = (QueryResultsInput)viewer_.getInput();
      viewer_.replace(input.getElement(index), index);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.IContentProvider#dispose()
     */
    public void dispose() {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IContentProvider#inputChanged(org.eclipse.jface
     * .viewers.Viewer, java.lang.Object, java.lang.Object)
     */
    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
    }

  }

  private static class ResultTableLabelProvider implements ITableLabelProvider {

    private TableViewer viewer_;

    ResultTableLabelProvider(TableViewer viewer) {
      viewer_ = viewer;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.ITableLabelProvider#getColumnImage(java.lang
     * .Object, int)
     */
    public Image getColumnImage(Object element, int columnIndex) {
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
      String res = "";
      if (element == null)
        return res;

      QueryResultsInput input = (QueryResultsInput)viewer_.getInput();
      try {
        Object columnValue = input.getColumnValue(element, columnIndex);
        if (columnValue != null)
          res = columnValue.toString();
      }
      catch (ColumnNotFoundException e) {
        // TODO: MGH - Should this be logged
      }
      catch (ColumnValueNotAvailableException e) {
        // TODO: MGH - Should this be logged
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
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.IBaseLabelProvider#dispose()
     */
    public void dispose() {

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.jface.viewers.IBaseLabelProvider#isLabelProperty(java.lang
     * .Object, java.lang.String)
     */
    public boolean isLabelProperty(Object element, String property) {
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
  }

  static private class ResultsTabFolderResizeHandler implements ControlListener {
    ResultsPanel parent_;

    ResultsTabFolderResizeHandler(ResultsPanel p) {
      parent_ = p;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.swt.events.ControlListener#controlMoved(org.eclipse.swt
     * .events .ControlEvent)
     */
    public void controlMoved(ControlEvent e) {
      // MGH: Do nothing; let the system handle it
      // System.out.println( "ResultsTabFolderResizeHandler.controlMoved"
      // );

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.eclipse.swt.events.ControlListener#controlResized(org.eclipse.swt
     * .events.ControlEvent)
     */
    public void controlResized(ControlEvent e) {
      Rectangle rcClntArea = parent_.getClientArea();
      int clntWd = rcClntArea.width;
      int clntHt = rcClntArea.height;

      if (e.widget == parent_.resultsContainer_) {
        // System.out.println(
        // "ResultsTabFolderResizeHandler.controlResized for txtQueryEntry_"
        // );
        parent_.resultsContainer_.setBounds(0, 0, clntWd, clntHt);
      }
    }
  } // ResultsTabFolderResizeHandler

}
