package com.gemstone.gemfire.mgmt.DataBrowser.ui.model;

import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;

public class QueryResultsInput implements ResultsInput {

  private QueryResult queryResults_;

  private IntrospectionResult metaData_;

  private Object[] elements_;

  private TableViewer viewer_;

  public QueryResultsInput(TableViewer vwr, QueryResult qryRslt, IntrospectionResult mtdata) {
    viewer_ = vwr;
    queryResults_ = qryRslt;
    metaData_ = mtdata;
    elements_ = queryResults_.getQueryResult(metaData_).toArray();
  }

  public Object getElement(int index) {
    return elements_[index];
  }

  public IntrospectionResult getMetaData() {
    return metaData_;
  }

  public int getSize() {
    return elements_.length;
  }

  public Object getColumnValue(Object element, int colIndex)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    Object columnValue = metaData_.getColumnValue(element, colIndex);
    return columnValue;
  }


  public Object getSelectedObject() {
    Object selectedObject = null;
    StructuredSelection selection= (StructuredSelection)viewer_.getSelection(); 
    if(selection != null){
      Object firstElement = selection.getFirstElement();
      selectedObject = firstElement;
    }
    return selectedObject;
  }
}
