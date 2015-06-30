/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.viewers.ColumnPixelData;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TableColumn;

import com.gemstone.gemfire.cache.query.QueryStatistics;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;

/**
 * @author mghosh
 *
 */
public class StatisticsPanel extends Composite {
  private TableViewer        tblCounters_   = null;

  private final String[]     dummyColLabels = { "Counter", "Value", };

  // TODO MGH - remove these as the code develops
  private final static int[] dummyColWidths = { 100, 350 };

  /**
   * @param parent
   * @param style
   */
  public StatisticsPanel(Composite parent, int style) {
    super(parent, style);
    GridLayout fl = new GridLayout();
    this.setLayout(fl);
    createTable();
  }

  private void createTable() {
    tblCounters_ = new TableViewer(this, SWT.H_SCROLL | SWT.V_SCROLL);

    TableLayout lytTable = new TableLayout();

    int columnCount = dummyColLabels.length;

    for (int i = 0; i < columnCount; i++) {
      lytTable.addColumnData(new ColumnPixelData(dummyColWidths[i], true));
    }

    tblCounters_.getTable().setLayout(lytTable);
    tblCounters_.getTable().setHeaderVisible(true);
    tblCounters_.getTable().setLinesVisible(true);

    GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
    tblCounters_.getTable().setLayoutData(gd);
    for (int i = 0; i < columnCount; i++) {
      String colName = dummyColLabels[i];
      TableColumn col = new TableColumn(tblCounters_.getTable(), SWT.NONE, i);
      col.setText(colName);
      col.setResizable(true);
    }

    tblCounters_.setContentProvider(new StatisticsTableContentProvider(
        tblCounters_));
    tblCounters_.setLabelProvider(new StatisticTableLabelProvider());
    tblCounters_.setInput(null);
  }

  public void showResults(Object oResults) {
    if (!(oResults instanceof QueryResult))
      return;// only QueryResult is expected

    QueryStatistics queryStatistics = ((QueryResult) oResults)
        .getQueryStatistics();
    tblCounters_.setInput(queryStatistics);
  }

  void handleEventForConnection() {
    if (tblCounters_ != null)
      tblCounters_.setInput(null);
  }

  private static class StatisticsTableContentProvider implements
      IStructuredContentProvider {

    private TableViewer viewer_;

    StatisticsTableContentProvider(TableViewer viewer) {
      viewer_ = viewer;
    }

    public void dispose() {
      // TODO Auto-generated method stub

    }

    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
      // TODO Auto-generated method stub

    }

    public Object[] getElements(Object inputElement) {
      // TODO Auto-generated method stub
      return new Object[] { inputElement };
    }

  }

  private static class StatisticTableLabelProvider implements
      ITableLabelProvider {

    StatisticTableLabelProvider() {

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

      Long columnValue;
      if (columnIndex == 0) {
        columnValue = Long.valueOf(((QueryStatistics) element).getNumExecutions());
        if (columnValue != null)
          res = columnValue.toString();
      } else if (columnIndex == 1) {
        columnValue = Long.valueOf(((QueryStatistics) element).getTotalExecutionTime());
        if (columnValue != null)
          res = columnValue.toString();
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

}
