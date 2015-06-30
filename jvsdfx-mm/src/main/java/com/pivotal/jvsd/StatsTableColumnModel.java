package com.pivotal.jvsd;

import java.util.ArrayList;
import java.util.Enumeration;
import javax.swing.ListSelectionModel;
import javax.swing.event.TableColumnModelListener;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import org.apache.commons.collections.IteratorUtils;

/**
 *
 * @author Vince Ford
 */
public class StatsTableColumnModel implements TableColumnModel {

	ArrayList<TableColumn> tca;

	public void addColumn(TableColumn tc) {
		tca.add(tc);
	}

	public void removeColumn(TableColumn tc) {
		tca.remove(tc);
	}

	public void moveColumn(int i, int i1) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void setColumnMargin(int i) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getColumnCount() {
		return tca.size();
	}

	public Enumeration<TableColumn> getColumns() {
		return IteratorUtils.asEnumeration(tca.iterator());
	}

	public int getColumnIndex(Object o) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public TableColumn getColumn(int i) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getColumnMargin() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getColumnIndexAtX(int i) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getTotalColumnWidth() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void setColumnSelectionAllowed(boolean bln) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean getColumnSelectionAllowed() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int[] getSelectedColumns() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getSelectedColumnCount() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void setSelectionModel(ListSelectionModel lsm) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public ListSelectionModel getSelectionModel() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void addColumnModelListener(TableColumnModelListener tl) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void removeColumnModelListener(TableColumnModelListener tl) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
