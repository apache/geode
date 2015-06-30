package com.gemstone.gemfire.mgmt.DataBrowser.ui.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ErrorEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.RowDeleted;

/**
 * 
 * @author mjha
 *
 */
public class CQueryResultsInput implements ResultsInput {
  
  public static final int NO_PRIMARY_COL_SELECTED = -1;
  private CQResult queryResults;
  private IntrospectionResult metaData;
  private TableViewer viewer;  
  private Map< Object, Object > prim_col_to_key_map;  
  private int colIndex;

  public CQueryResultsInput(TableViewer vwr, CQResult qryRslt, IntrospectionResult mtdata, int index) {
    viewer= vwr;
    queryResults = qryRslt;
    metaData = mtdata;
    colIndex = index;
    this.prim_col_to_key_map = Collections.synchronizedMap(new HashMap< Object, Object >());
  }
  
  public IntrospectionResult getMetaData() {
    return metaData;
  }
  
  public int getPrimaryColIndex(){
    return colIndex;
  }

  public Object getColumnValue(Object element, int colIdx)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    Object columnValue = metaData.getColumnValue(element, colIdx);
    return columnValue;
  }

 
  public Object getSelectedObject() {
    Object selectedObject = null;
    StructuredSelection selection= (StructuredSelection)viewer.getSelection(); 
    if(selection != null){
      EventData firstElement = (EventData)selection.getFirstElement();
      if(firstElement != null){
        selectedObject = firstElement.getValue();
      }
    }
    return selectedObject;
  }
  
  public void processEvent(ICQEvent cqEvent) {
    if (!(cqEvent instanceof ErrorEvent)) {
      EventData data = cqEvent.getEventData();
      
      if((data == null) || (!data.getIntrospectionResult().equals(metaData))) {
       return; 
      }
      
      if (cqEvent instanceof RowDeleted) {
        viewer.remove(cqEvent.getEventData());

      }
      else if (colIndex == NO_PRIMARY_COL_SELECTED) {
        if (prim_col_to_key_map.containsKey(data.getKey())) {
          viewer.update(data, null);
        }
        else {
          prim_col_to_key_map.put(data.getKey(), data.getKey());
          viewer.add(data);
        }

      }
      else {
        Object event_key = data.getKey();
        IntrospectionResult result = data.getIntrospectionResult();
        if (metaData.equals(result)) {
          Object value = data.getValue();
          if (value != null) {
            try {
              Object prim_col_value = metaData.getColumnValue(value, colIndex);
              Object key = prim_col_to_key_map.get(prim_col_value);
              EventData oldData = queryResults.getValueForKey(key);

              // Previous entry is available.
              if (key != null) {
                // If keys are not same, delete the old entry and add new entry.
                if (!key.equals(event_key)) {
                  //Ensure that we are not updating the row with the stale data.
                  if((oldData == null) || (oldData.compareTo(data) < 0)) {
                    //Remove the old data.
                    if(oldData != null)
                     viewer.remove(oldData);
                    // Add the new value...
                    prim_col_to_key_map.put(prim_col_value, event_key);
                    viewer.add(data);                    
                  } 
                }
                else {
                  // Since keys are same...just update the values.
                  viewer.update(data, null);
                }

              }
              else {
                // Previous entry not available. Add a new entry...
                prim_col_to_key_map.put(prim_col_value, event_key);
                viewer.add(data);
              }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

  }
  
  public void dispose() {
    queryResults = null;
    metaData = null;
    viewer = null;
  }
}
