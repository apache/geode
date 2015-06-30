package com.pivotal.jvsd;

import java.util.ArrayList;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

/**
 *
 * @author Vince Ford
 */
public class StatsTableModel implements TableModel {

	String[] columnNames = {"Row", "Start Time", "File", "Samples", "PID", "Type", "Name"};

	ArrayList tableData;

	public StatsTableModel() {
		tableData = new ArrayList();
	}

	public int getRowCount() {
		return tableData.size();
	}

	public int getColumnCount() {
		return columnNames.length;
	}

	public String getColumnName(int i) {
		return columnNames[i];
	}

	public Class<?> getColumnClass(int i) {
		return String.class;
	}

	public boolean isCellEditable(int i, int i1) {
		return false;
	}

	public Object getValueAt(int i, int i1) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void setValueAt(Object o, int i, int i1) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void addTableModelListener(TableModelListener tl) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void removeTableModelListener(TableModelListener tl) {
		//null op right now
	}

	public void addRow(Object[] rowdata) {
		tableData.add(rowdata);
	}

}
