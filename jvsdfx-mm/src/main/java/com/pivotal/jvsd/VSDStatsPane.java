package com.pivotal.jvsd;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

/**
 *
 * @author Vince Ford
 */
public class VSDStatsPane extends JPanel {

	JList stats = null;

	public VSDStatsPane() {
		super();

		this.setLayout(new BorderLayout());
		stats = new JList();
		stats.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		stats.setLayoutOrientation(JList.VERTICAL);
		stats.setVisibleRowCount(8);
		JScrollPane jsp = new JScrollPane(stats);
		JPanel panel = new JPanel();
		this.add(jsp, BorderLayout.CENTER);
		this.add(panel, BorderLayout.SOUTH);
		stats.setModel(new DefaultListModel());
	}

  public List<String> getSelectedValues() {
    return stats.getSelectedValuesList();
  }

	void clear() {
		((DefaultListModel) (stats.getModel())).clear();
	}

	void update(List<String> statsValue) {
		clear();
		DefaultListModel dlm = (DefaultListModel) stats.getModel();
		for (String x : statsValue) {
			dlm.addElement(x);
		}
	}
}
