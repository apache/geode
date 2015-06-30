/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.viewers.ColumnPixelData;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.ControlListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.cache.query.CqStatistics;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ErrorEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.RowAdded;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.model.CQueryResultsInput;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.tree.ExploreAction;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mjha
 * 
 */
public class CQResultsPanel extends Composite {
  private static final int  WIDTH       = 100;

  private List<TableViewer> tblResults_ = null;
  private CTabFolder        resultsContainer_;
  private CQQuery           query;
  StatisticsPanel           statsPanel;

  private Text              exceptions;

  /**
   * @param parent
   * @param style
   */
  public CQResultsPanel(Composite parent, int style) {
    super(parent, style);

    GridLayout layout = new GridLayout();
    layout.numColumns = 1;

    this.setLayout(layout);
    
    GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
    
    SashForm sashForm= new SashForm(this, SWT.FLAT | SWT.VERTICAL);
    layout = new GridLayout();
    layout.numColumns = 1;
    sashForm.setLayout(layout);
    sashForm.setLayoutData(data);

    Composite topComposite = new Composite(sashForm, SWT.NONE);
    layout = new GridLayout();
    layout.numColumns = 1;
    topComposite.setLayout(layout);
    data = new GridData(SWT.FILL, SWT.FILL, true, true);
    topComposite.setLayoutData(data);
    
    data = new GridData(SWT.FILL, SWT.FILL, true, false);
    ScrolledComposite scrolledComposite = new ScrolledComposite(topComposite,SWT.H_SCROLL| SWT.V_SCROLL);
    scrolledComposite.setLayoutData(data);
    GridLayout gridLayout = new GridLayout();
    scrolledComposite.setLayout(gridLayout);
    scrolledComposite.setMinWidth(400);
    scrolledComposite.setMinHeight(40);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    
    data = new GridData(SWT.FILL, SWT.FILL, true, false);
    statsPanel = new StatisticsPanel(scrolledComposite);
    statsPanel.setLayoutData(data);

    scrolledComposite.setContent(statsPanel);
    
    data = new GridData(SWT.FILL, SWT.FILL, true, true);

    Composite subPanel = new Composite(topComposite, SWT.NONE);
    subPanel.setLayoutData(data);

    GridLayout fl = new GridLayout();
    subPanel.setLayout(fl);
    resultsContainer_ = new CTabFolder(subPanel, SWT.NONE);
    resultsContainer_.setBorderVisible(true);

    resultsContainer_.setSimple(false);
    resultsContainer_.setUnselectedCloseVisible(true);
    resultsContainer_.setUnselectedImageVisible(true);
    // resultsContainer_.addControlListener(new
    // ResultsTabFolderResizeHandler(this));
    resultsContainer_
        .setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    fl = new GridLayout();
    resultsContainer_.setLayout(fl);

    addDummyTabs();

    subPanel.layout();
    // subPanel.pack();

    {
      Group group = new Group(sashForm, SWT.SHADOW_ETCHED_IN);
      group.setText("CQ Console"); 
      group.setLayoutData(new GridData(SWT.FILL, SWT.BEGINNING, true, true));
      GridLayout lytGrpDisc = new GridLayout();
      lytGrpDisc.numColumns = 1;
      group.setLayout(lytGrpDisc);

      exceptions = new Text(group, SWT.MULTI | SWT.READ_ONLY | SWT.LEFT
          | SWT.BORDER | SWT.V_SCROLL);
      data = new GridData(SWT.FILL, SWT.FILL, true, true);
      exceptions.setLayoutData(data);
    }
    
    sashForm.setWeights(new int[]{80,20});

    this.layout();
    this.pack();

  }

  private void addDummyTabs() {
    int iStyle = SWT.NONE;
    iStyle |= SWT.BORDER;
    CTabItem ti = new CTabItem(resultsContainer_, iStyle, 0);
    ti.setText("Results"); 
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

  public void setQuery(CQQuery qry) {
    disposeOldResults();
    addDummyTabs();
    query = qry;
  }

  private void disposeOldResults() {
    CTabItem[] items = resultsContainer_.getItems();
    for (int i = 0; i < items.length; i++) {
      items[i].getControl().dispose();
      items[i].dispose();
    }
    if (tblResults_ != null) {
      tblResults_.clear();
      tblResults_ = null;
    }
    if (exceptions != null && !exceptions.isDisposed()) {
      exceptions.setText(""); 
    }
  }

  public void close() {
  }

  public void processCqEvent(ICQEvent cqEvent) {
    if (cqEvent instanceof ErrorEvent) {
      if (cqEvent.getThrowable() != null) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        cqEvent.getThrowable().printStackTrace(pw);
        exceptions.append(sw.toString());

        try {
          pw.close();
          sw.close();
        } catch (IOException e) {
        }
      }
    } else {
      if (tblResults_ != null) {
        for (int i = 0; i < tblResults_.size(); i++) {
          TableViewer tableViewer = tblResults_.get(i);
          synchronized (tableViewer) {
            CQueryResultsInput input = (CQueryResultsInput) tableViewer
                .getInput();
            if( null != input ) {
              input.processEvent(cqEvent);
            }
          }
        }
      }
    }
    statsPanel.update(query.getStatistics());
  }

  public void processNewTypeEvent(final IntrospectionResult result) {
    if (tblResults_ == null)
      disposeOldResults();

    int iStyle = SWT.NONE;
    iStyle |= SWT.BORDER;
    int index = resultsContainer_.getItemCount();
    
    // TODO FINDBUG MGH - false positive reported 'Dead store to local variable' on ti
    CTabItem ti = new CTabItem(resultsContainer_, iStyle, index++);
    CQDataTab dataTab = new CQDataTab(resultsContainer_, result);
    dataTab.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    ti.setText(result.getJavaTypeName());
    ti.setControl(dataTab);
    CQResultsPanel.this.resultsContainer_.setSelection(0);
  }

  static private class ResultsTabFolderResizeHandler implements ControlListener {
    CQResultsPanel parent_;

    ResultsTabFolderResizeHandler(CQResultsPanel p) {
      parent_ = p;
    }

    public void controlMoved(ControlEvent e) {
    }

    public void controlResized(ControlEvent e) {
      Rectangle rcClntArea = parent_.getClientArea();
      int clntWd = rcClntArea.width;
      int clntHt = rcClntArea.height;

      if (e.widget == parent_.resultsContainer_) {
        parent_.resultsContainer_.setBounds(0, 0, clntWd, clntHt);
      }
    }
  }

  private static class StatisticsPanel extends Composite {

    private Label numEvents;
    private Label numInserts;
    private Label numUpdates;
    private Label numDeletes;

    public StatisticsPanel(Composite parent) {
      super(parent, SWT.NONE);
      GridLayout layout = new GridLayout();
      layout.numColumns = 4;
      layout.makeColumnsEqualWidth = false;
      this.setLayout(layout);

      Label temp = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      temp.setText("Number of Events :"); 
      GridData gridData = new GridData(SWT.BEGINNING, SWT.FILL, false, false);
      temp.setLayoutData(gridData);

      numEvents = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      numEvents.setText("");
      numEvents.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

      temp = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      temp.setText("Number of Inserts : ");
      temp.setLayoutData(new GridData(SWT.BEGINNING, SWT.FILL, false, false));

      numInserts = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      numInserts.setText("");
      numInserts.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

      temp = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      temp.setText("Number of Updates : ");
      temp.setLayoutData(new GridData(SWT.BEGINNING, SWT.FILL, false, false));

      numUpdates = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      numUpdates.setText("");
      numUpdates.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

      temp = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      temp.setText("Number of Deletes : "); 
      temp.setLayoutData(new GridData(SWT.BEGINNING, SWT.FILL, false, false));

      numDeletes = new Label(this, SWT.HORIZONTAL | SWT.LEFT);
      numDeletes.setText(""); 
      numDeletes.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));

      this.layout();
//      this.pack();
    }

    public void update(CqStatistics statistics) {
      numEvents.setText(String.valueOf(statistics.numEvents()));
      numInserts.setText(String.valueOf(statistics.numInserts()));
      numUpdates.setText(String.valueOf(statistics.numUpdates()));
      numDeletes.setText(String.valueOf(statistics.numDeletes()));
    }
  }

  private class CQDataTab extends Composite {
    private Combo       columnSelection;
    private TableViewer dataTable;

    public CQDataTab(Composite parent, final IntrospectionResult metaInfo) {
      super(parent, SWT.NONE);

      GridLayout layout = new GridLayout();
      layout.numColumns = 1;
      this.setLayout(layout);

      columnSelection = new Combo(this, SWT.DROP_DOWN);
      columnSelection.setLayoutData(new GridData(SWT.BEGINNING, SWT.BEGINNING,
          false, false));

      // TODO MGH - make this 'un-anonymous'
      columnSelection.addSelectionListener(new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
          int index = columnSelection.getSelectionIndex();

          int colIndex;
          if (index == 0) {
            colIndex = CQueryResultsInput.NO_PRIMARY_COL_SELECTED;
          } else {
            colIndex = index - 1;
          }

          CQueryResultsInput currentInput = (CQueryResultsInput) dataTable
              .getInput();

          if (currentInput.getPrimaryColIndex() != colIndex) {
            CQueryResultsInput newInput = new CQueryResultsInput(dataTable,
                query.getQueryResult(), metaInfo, colIndex);

            synchronized (dataTable) {
              dataTable.setInput(newInput);
            }

            Set<Object> keys = query.getQueryResult().getKeys();

            Iterator<Object> iter = keys.iterator();
            while (iter.hasNext()) {
              Object key = iter.next();
              EventData value = query.getQueryResult().getValueForKey(key);
              if (value != null) {
                newInput.processEvent(new RowAdded(value));
              }
            }
          }
        }
      });

      // Add data in Combo.
      columnSelection.add("None");

      int columnCount = metaInfo.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        try {
          columnSelection.add(metaInfo.getColumnName(i));
        } catch (ColumnNotFoundException e) {
          LogUtil.error("Exception when adding column names in CQDataTab()", e );
        }
      }

      columnSelection.select(0);

      // Add columns in the table.

      dataTable = new TableViewer(this, SWT.SINGLE | SWT.FULL_SELECTION
          | SWT.H_SCROLL | SWT.V_SCROLL);
      dataTable.getTable().setLayoutData(
          new GridData(SWT.FILL, SWT.FILL, true, true));
      TableLayout lytTable = new TableLayout();
      columnCount = metaInfo.getColumnCount();

      for (int i = 0; i < columnCount; i++) {
        lytTable.addColumnData(new ColumnPixelData(WIDTH, true));
      }

      dataTable.getTable().setLayout(lytTable);
      dataTable.getTable().setHeaderVisible(true);
      dataTable.getTable().setLinesVisible(true);

      GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
      dataTable.getTable().setLayoutData(gd);
      for (int i = 0; i < columnCount; i++) {
        try {
          String colName = metaInfo.getColumnName(i);
          TableColumn col = new TableColumn(dataTable.getTable(), SWT.NONE, i);
          col.setText(colName);
          col.setResizable(true);
        } catch (ColumnNotFoundException e) {
          LogUtil.error( "Exception when adding column in CQDataTab()", e );
          continue;
        }
      }

      CQTableContentProvider prov = new CQTableContentProvider(dataTable);
      dataTable.setContentProvider(prov);
      dataTable.setLabelProvider(new ResultTableLabelProvider(dataTable));

      CQueryResultsInput queryResultsInput = new CQueryResultsInput(dataTable,
          query.getQueryResult(), metaInfo, -1);
      dataTable.setInput(queryResultsInput);
      
      ExploreAction exploreAction = new ExploreAction(this.getShell(), dataTable);
      MenuManager manager = new MenuManager();
      dataTable.getTable().setMenu(manager.createContextMenu(dataTable.getTable()));
      manager.add(exploreAction);


      if (tblResults_ == null)
        tblResults_ = new ArrayList<TableViewer>();

      tblResults_.add(dataTable);

    }

  }

  public static class CQTableContentProvider implements IStructuredContentProvider {

    private TableViewer viewer;

    public CQTableContentProvider(TableViewer vwr) {
      super();
      viewer = vwr;

    }

    public Object[] getElements(Object inputElement) {
      return new Object[0];
    }

    public void dispose() {
      CQueryResultsInput input = (CQueryResultsInput) viewer.getInput();
      input.dispose();
    }

    public void inputChanged(Viewer vwr, Object oldInput, Object newInput) {

    }
  }

  private static class ResultTableLabelProvider implements ITableLabelProvider {
    private TableViewer viewer_;

    ResultTableLabelProvider(TableViewer viewer) {
      viewer_ = viewer;
    }

    public Image getColumnImage(Object element, int columnIndex) {
      return null;
    }

    public String getColumnText(Object element, int columnIndex) {
      String res = "";
      if (element == null)
        return res;

      EventData data = (EventData) element;
      Object value = data.getValue();
      CQueryResultsInput input = (CQueryResultsInput) viewer_.getInput();
      try {
        Object columnValue = input.getColumnValue(value, columnIndex);
        if (columnValue != null)
          res = columnValue.toString();
      } catch (ColumnNotFoundException e) {
        return "N/A";
      } catch (ColumnValueNotAvailableException e) {
        return "N/A"; 
      }

      return res;
    }

    public void addListener(ILabelProviderListener listener) {
    }

    public void dispose() {
    }

    public boolean isLabelProperty(Object element, String property) {
      return false;
    }

    public void removeListener(ILabelProviderListener listener) {
    }
  }
}
