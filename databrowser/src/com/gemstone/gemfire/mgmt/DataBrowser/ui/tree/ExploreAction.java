package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QueryResultExporter;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.model.ResultsInput;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class ExploreAction extends Action implements IDoubleClickListener {
  Shell parent;
  TableViewer tableViewer;

  public ExploreAction(Shell prnt, TableViewer tblVwr) {
    setText("Explore...");
    parent = prnt;
    tableViewer = tblVwr;
  }

  @Override
  public void run() {
    ResultsInput input = (ResultsInput)tableViewer.getInput();
    IntrospectionResult metaInfo = input.getMetaData();
    Object obj = input.getSelectedObject();
    if(obj != null){
      ObjectTreeModelImpl impl = new ObjectTreeModelImpl();
      QueryResultExporter exporter = new QueryResultExporter(impl, -1);

      try {
        exporter.exportObject("Result", metaInfo, obj);

        ObjectImage root = (ObjectImage)impl.getResultDocument();

        ObjectExplorer explorer = new ObjectExplorer(parent);
        explorer.updateModel(root);
        explorer.open();
      }
      catch (Exception e) {
        // TODO: Proper error handling.
        // TODO MGH - remove this and use logging or some other mechanism
        LogUtil.error("Error while exploring table row", e);
      }
    }
  }

  public void doubleClick(DoubleClickEvent event) {
    run();
  }
}
