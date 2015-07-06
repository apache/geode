package com.pivotal.jvsd;

import com.pivotal.jvsd.fx.VSDChartWindow;
import com.pivotal.jvsd.stats.StatFileManager;
import com.pivotal.jvsd.stats.StatFileParser;
import com.pivotal.jvsd.stats.StatFileParser.StatValue;
import com.pivotal.jvsd.stats.StatFileWrapper;
import com.pivotal.jvsd.stats.Utility;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javafx.embed.swing.JFXPanel;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;

/**
 *
 * @author Vince Ford
 */
public class VSDMainWindow extends JFrame
    implements ActionListener, ListSelectionListener {

	private JMenuBar menubar;
	private final int CHARTMENU_POSITION = 3;
	private ChartMenuActionListener chartMenuListener = null;
	private JTable table;
	private DefaultTableModel tableModel;
	private StatFileManager statFiles;
	private VSDStatsPane statsPane;
	private ArrayList<VSDChartWindow> chartWindows = null;
	private JMenuItem exportMenuItem = null;

  private JButton buttonNewChart = null;
  private JButton buttonAddToChart = null;
  private JComboBox chartList = null;

  private boolean hideZeroStats = true;

  private static final Object[] ROW_HEADERS =
      {"Row", "Start Time", "File", "Samples", "PID", "Type", "Name"};

  public VSDMainWindow(StatFileManager sfm) {
    super();
    
    // HACK - init JFX
    new JFXPanel();
    
    Utility.initScreenSize();
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    statFiles = sfm;
    chartWindows = new ArrayList<VSDChartWindow>();
    createMenus();
    createStatsList();
    createBottomPane();
    createChartButtons();
    this.setSize(640,  800);
//    this.setSize((int) (Utility.getHorizontalScreenSize() * 0.75),
//        ((int) (Utility.getVerticalScreenSize() * 0.75)));
    this.setTitle("jVSD");
    if (statFiles.length() > 0) {
      statFiles.updateTableView(table);
      exportMenuItem.setEnabled(true);
    }
  }

  /**
   * Returns a list of StatValues depending on the stats and stat names
   * selected.
   */
  private List<StatValue> getSelectedStatValues() {
    List<StatValue> results = new ArrayList<StatValue>();
    String type = null;
    String name = null;
    int filenum = 0;
    int[] indexes = table.getSelectedRows();

    for (int rowIdx : indexes) {
      int index = table.convertRowIndexToModel(rowIdx);
      for (int x = 0; x < tableModel.getColumnCount(); x++) {
        if (tableModel.getColumnName(x).equals("Type")) {
          type = (String) tableModel.getValueAt(index, x);
        } else if (tableModel.getColumnName(x).equals("File")) {
          filenum = ((Integer) tableModel.getValueAt(index, x)).intValue();
        } else if (tableModel.getColumnName(x).equals("Name")) {
          name = (String) tableModel.getValueAt(index, x);
        }
      }

      StatFileWrapper stw = statFiles.getFile(filenum - 1);
      StatFileParser.ResourceInst ri = stw.getResource(type, name);

      for (String statName : statsPane.getSelectedValues()) {
        results.add(ri.getStatValue(statName));
      }
    }

    return results;
  }

	private void createMenus() {
		menubar = new JMenuBar();
		JMenuItem item;
		JMenu menu;
		//***********
		//File Menu
		menu = Utility.createMenu("File", menubar);
		Utility.createMenuItem("Open", menu, this);
		exportMenuItem = Utility.createMenuItem("Export to CSV", menu, this);
		exportMenuItem.setEnabled(false);
		item = Utility.createMenuItem("Connect", menu, this);
		item.setEnabled(false);
		Utility.createMenuItem("Quit", menu, this);
		//***********
		//Edit Menu
		menu = Utility.createMenu("Edit", menubar);
		Utility.createMenuItem("Undo", menu, this);
		Utility.createMenuItem("Cut", menu, this);
		Utility.createMenuItem("Copy", menu, this);
		Utility.createMenuItem("Paste", menu, this);
		Utility.createMenuItem("Select All", menu, this);
		menu.addSeparator();
		Utility.createMenuItem("Search", menu, this);
		menu.addSeparator();
		Utility.createMenuItem("Preferences", menu, this);
		menu.setEnabled(false);
		//***********
		//View Menu
		menu = Utility.createMenu("View", menubar);
		JCheckBoxMenuItem cbMenuItem = new JCheckBoxMenuItem("Hide zero stats");
		cbMenuItem.addItemListener(new ItemListener() {
      @Override
      public void itemStateChanged(ItemEvent e) {
//        if (e.getStateChange() == ItemEvent.SELECTED) {
//          hideZeroStats = true;
//        } else {
//          hideZeroStats = false;
//        }
//        updateStatList();
      }});

		menu.add(cbMenuItem);
		

//		menu.setEnabled(false);
		//***********
		//Charts Menu
		menu = Utility.createMenu("Charts", menubar);
		menu.setEnabled(false);
		//***********
		//Templates Menu
		menu = Utility.createMenu("Template", menubar);
		Utility.createMenuItem("New Template", menu, this);
		menu.addSeparator();
		menu.setEnabled(false);
    //predefined templates will be added below this separator

		//***********
		//About Menu
		menu = Utility.createMenu("About", menubar);
		Utility.createMenuItem("About jVSD", menu, this);
		item = Utility.createMenuItem("Help", menu, this);
		item.setEnabled(false);
		this.setJMenuBar(menubar);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
    VSDChartWindow vcw = null;
		Object o = e.getSource();

		if (o instanceof JMenuItem) {
			JMenuItem mi = (JMenuItem) o;
			String menuItem = mi.getActionCommand();
			if ("Quit".equals(menuItem)) {
					System.exit(0);
      } else if ("Open".equals(menuItem)) {
			  openFile();
      } else if ("Export to CSV".equals(menuItem)) {
			  saveFile();
      } else if ("About JVSD".equals(menuItem)) {
			  openAboutDialog();
			}
		} else if (o == buttonNewChart || o == buttonAddToChart) {
      List<StatValue> mystats = getSelectedStatValues();
      if (mystats.size() > 0 && o == buttonNewChart) {
        StatValue stat = null; //mystats.remove(0);

//        vcw = new VSDChartWindow(stat, this);
//        vcw.start();
        addChartWindow(vcw);
        chartList.addItem(vcw.getTitle());
        chartList.setSelectedItem(vcw.getTitle());
        //vcw.setVisible(true);
      } else if (o == buttonAddToChart) {
        String chart = (String) chartList.getSelectedItem();
        if (chart != null) {
          vcw = getChartWindow(chart);
        }
      }

//      vcw.addToChart(mystats);

      if (chartList.getItemCount() > 0) {
        buttonAddToChart.setEnabled(true);
      } else {
        buttonAddToChart.setEnabled(false);
      }
    }
	}

	private void createStatsList() {
		this.getContentPane().setLayout(new BorderLayout());

		table = new JTable() {
			@Override
			public boolean isCellEditable(int rowIndex, int ColIndex) {
				return false;
			}
		};

		table.setAutoCreateRowSorter(true);
//		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		JScrollPane scrollpane = new JScrollPane(table);

		this.getContentPane().add(scrollpane, BorderLayout.NORTH);
		tableModel = (DefaultTableModel) table.getModel();
		int paneWidth = scrollpane.getWidth();
		// TODO Fix this - paneWidth = 0

		// Set some specific column widths
		int[] widths = {(int)(paneWidth * 0.05),
				(int)(paneWidth * 0.2),
				(int)(paneWidth * 0.2),
				(int)(paneWidth * 0.05),
				(int)(paneWidth * 0.1),
				(int)(paneWidth * 0.2),
				(int)(paneWidth * 0.2)
		};
		for (int i = 0; i < widths.length; i++) {
			TableColumn tc = new TableColumn(i);
			tc.setPreferredWidth(widths[i]);
			tableModel.addColumn(tc);
		}

		tableModel.setColumnIdentifiers(ROW_HEADERS);

		table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    table.getSelectionModel().addListSelectionListener(this);
  }

	private void openFile() {
		final JFileChooser fc = new JFileChooser();
		int returnVal = fc.showOpenDialog(this);
		switch (returnVal) {
			case JFileChooser.APPROVE_OPTION:
				File file = fc.getSelectedFile();
				statFiles.addFile(file);
				statFiles.updateTableView(table);
				exportMenuItem.setEnabled(true);
				break;
			case JFileChooser.CANCEL_OPTION:
				break;
			case JFileChooser.ERROR_OPTION:
			default:
				break;
		}
	}

	private void createBottomPane() {
		statsPane = new VSDStatsPane();
		this.getContentPane().add(statsPane, BorderLayout.CENTER);
	}

  private void createChartButtons() {
    JPanel panel = new JPanel();
    buttonNewChart = new JButton("New Chart");
    buttonAddToChart = new JButton("Add to Chart");
    chartList = new JComboBox();
    buttonNewChart.addActionListener(this);
    buttonAddToChart.addActionListener(this);
    buttonAddToChart.setEnabled(false);
    panel.add(buttonNewChart);
    panel.add(buttonAddToChart);
    panel.add(chartList);
    this.getContentPane().add(panel, BorderLayout.SOUTH);
  }

  /**
   * This will update the stats pane with all the common stats of rows
   * selected in the main window.
   */
	private void updateStatList() {
		statsPane.clear();

    int[] rows = table.getSelectedRows();
    List<String> statNames = getStatNames(rows[0]);

    for (int i = 1; i < rows.length; i++) {
      List<String> tmpNames = getStatNames(rows[i]);
      Iterator<String> it = statNames.iterator();
      while (it.hasNext()) {
        String s = it.next();
        if (!tmpNames.contains(s)) {
          it.remove();
        }
      }
    }

    statsPane.update(statNames);
	}

  private List<String> getStatNames(int tableRowIndex) {
    List<String> statNames = null;
    String type = null;
    String name = null;
    int filenum = -1;
    int modelIndex = table.convertRowIndexToModel(tableRowIndex);

    for (int x = 0; x < tableModel.getColumnCount(); x++) {
      if (tableModel.getColumnName(x).equals("Type")) {
        type = (String) tableModel.getValueAt(modelIndex, x);
      } else if (tableModel.getColumnName(x).equals("File")) {
        filenum = ((Integer) tableModel.getValueAt(modelIndex, x)).intValue();
      } else if (tableModel.getColumnName(x).equals("Name")) {
        name = (String) tableModel.getValueAt(modelIndex, x);
      }
    }
    if (type != null && name != null && filenum != -1) {
      statNames = statFiles.getStats(filenum, type, name, hideZeroStats);
    }

    return statNames;
  }

	void addChartWindow(VSDChartWindow vcw) {
		chartWindows.add(vcw);
		if (this.chartMenuListener == null) {
			this.chartMenuListener = new ChartMenuActionListener(this.chartWindows);
		}
		Utility.
						createMenuItem(vcw.getTitle(), menubar.getMenu(CHARTMENU_POSITION), this.chartMenuListener);
	}

	void removeChartWindow(VSDChartWindow vcw) {
		chartWindows.remove(vcw);
		for (int x = 0; x < menubar.getMenu(CHARTMENU_POSITION).getItemCount(); x++) {
			if (menubar.getMenu(CHARTMENU_POSITION).getItem(x).getText().equals(vcw.
							getTitle())) {
				menubar.getMenu(CHARTMENU_POSITION).remove(x);
				break;
			}
		}

    chartList.removeItem(vcw.getTitle());
    if (chartList.getItemCount() == 0) {
      buttonAddToChart.setEnabled(false);
    }

		// TODO vcw.dispose();
	}

	VSDChartWindow getChartWindow(String chart) {
		VSDChartWindow vcw = findChartWindow(chart);

		return vcw;
	}

	private VSDChartWindow findChartWindow(String chart) {
		for (VSDChartWindow vcw : this.chartWindows) {
			if (vcw.getTitle().equals(chart)) {
				return vcw;
			}
		}
		return null;
	}

	private void saveFile() {
		JFileChooser fc = new JFileChooser();
		fc.setDialogType(JFileChooser.SAVE_DIALOG);
		int returnVal = fc.showSaveDialog(this);
		switch (returnVal) {
			case JFileChooser.APPROVE_OPTION:
				File file = fc.getSelectedFile();
				Utility.dumpCSV(statFiles, file);
				break;
			case JFileChooser.CANCEL_OPTION:
				break;
			case JFileChooser.ERROR_OPTION:
			default:
				break;
		}
	}

	private void openAboutDialog() {
		AboutJVSD dialog = new AboutJVSD(this, true);
		dialog.setVisible(true);
	}

  @Override
  public void valueChanged(ListSelectionEvent e) {
    if (!e.getValueIsAdjusting()) {
      updateStatList();
    }
  }
}
