package com.pivotal.jvsd;

import com.pivotal.jvsd.stats.StatFileParser.StatValue;
import com.pivotal.jvsd.stats.Utility;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Paint;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.awt.geom.Rectangle2D;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;

import com.pivotal.jvsd.util.PanelOverlay;
import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartMouseEvent;
import org.jfree.chart.ChartMouseListener;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.LegendItemEntity;
import org.jfree.chart.labels.XYToolTipGenerator;
import org.jfree.chart.panel.CrosshairOverlay;
import org.jfree.chart.plot.Crosshair;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleEdge;

/**
 *
 * @author Vince Ford
 */
public class VSDChartWindow extends JFrame
    implements ActionListener, WindowListener, ChartMouseListener, KeyListener {

	static int chartID = -1;
	private JMenuBar menubar = null;
	private VSDMainWindow mainWindow;
//	private ChartSidePanel valuePanel = null;
	private ArrayList<StatValue> statsArray = null;
	private JFreeChart chart = null;
	private ChartPanel cp = null;
	private AtomicInteger plotNumber;
  private CrosshairOverlay crosshairOverlay;
  private HUD hud;

  private static final float dash1[] = {5.0f};

  private static final BasicStroke dashed =
      new BasicStroke(0.5f,
          BasicStroke.CAP_BUTT,
          BasicStroke.JOIN_MITER,
          5.0f, dash1, 0.0f);

  private static final SimpleDateFormat X_AXIS_DATE_FORMAT =
      new SimpleDateFormat("HH:mm:ss.SSS");

  private static Color PLOT_BACKGROUND = new Color(220, 220, 220);
  private static Color SELECTED_SERIES = new Color(128, 0, 0);

  private XYSeriesCollection lastDataset = null;
  private Comparable lastSeries;
  private Paint lastPaint;

	static synchronized int updateChartID() {
		chartID++;
		return chartID;
	}

	public VSDChartWindow(StatValue stat, VSDMainWindow parentContainer) {
    this.mainWindow = parentContainer;
    String name = stat.getDescriptor().getName();
    this.setTitle("Chart" + updateChartID() + "_" + name);
    statsArray = new ArrayList<StatValue>();
    plotNumber = new AtomicInteger(0);
    plotNumber.getAndIncrement();
    initializeChartWindow();
    createChart(stat);
    this.addKeyListener(this);
	}

	private void initializeChartWindow() {
		this.setSize((int) (Utility.getHorizontalScreenSize() * 0.75),
						(int) (Utility.getVerticalScreenSize() * 0.75));
		this.addWindowListener(this);
		createMenus();
		this.getContentPane().setLayout(new BorderLayout());
//		this.valuePanel = new ChartSidePanel(this);
//		JScrollPane scrollpane = new JScrollPane(valuePanel);
//		scrollpane.
//						setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
//		this.getContentPane().add(scrollpane, BorderLayout.EAST);
	}

	private void createMenus() {
		menubar = new JMenuBar();
		JMenu menu = Utility.createMenu("Chart", menubar);
		Utility.createMenuItem("Close", menu, this);
		menu = Utility.createMenu("Edit", menubar);
		menu.setEnabled(false);
		menu = Utility.createMenu("Line", menubar);
		menu.setEnabled(false);
		menu = Utility.createMenu("Help", menubar);
		menu.setEnabled(false);
		this.setJMenuBar(menubar);
	}

	public void actionPerformed(ActionEvent e) {
		Object o = e.getSource();
		if (o instanceof JMenuItem) {
			JMenuItem mi = (JMenuItem) o;
			String menuItem = mi.getActionCommand();
			if (menuItem.equals("Close")) {
				this.setVisible(false);
				mainWindow.removeChartWindow(this);
			}
		} else {
			if (o instanceof JComboBox) {
				JComboBox cb = (JComboBox) o;
				if (cb.getName().equals("StatComboBox")) {
					String statname = (String) cb.getSelectedItem();
					StatValue sv = this.lookupIndexedStatName(statname);
//					valuePanel.setMax(sv.getSnapshotsMaximum());
//					valuePanel.setMin(sv.getSnapshotsMinimum());
//					valuePanel.setStdDev(sv.getSnapshotsStandardDeviation());
//					valuePanel.setMean(sv.getSnapshotsAverage());
//					valuePanel.setDescription(sv.getDescriptor().getDescription());
//					valuePanel.setUnits(sv.getDescriptor().getUnits());
//					valuePanel.setSamples(sv.getSnapshotsSize());
//					valuePanel.setFilterComboBox(sv.getFilter());
//					valuePanel.updateValues();
				}
			}
		}
	}

  private XYSeries createXYSeries(StatValue sv) {
    long[] timestamps = sv.getRawAbsoluteTimeStamps();
    double[] dataValue;
    long timeSampleAverage = Utility.computeTimeSampleAverage(timestamps);
    long samplePeriod = timeSampleAverage / 1000;
    XYSeries sample = new XYSeries(getIndexedStatName(sv));

    timestamps = sv.getRawAbsoluteTimeStamps();
    dataValue = sv.getRawSnapshots();
    if (sv.getFilter() == sv.FILTER_PERSEC) {

      for (int d = 0; d < timestamps.length; d++) {
        long timestamp = timestamps[d];
        double value = dataValue[d];
        if (d == 0) {
          value = Utility.
              computePerSecondValue(timestamp,
                  timestamp - timeSampleAverage, value,
                  sv.getSnapshotsAverage(), samplePeriod);
          sample.add(timestamp, value);
        } else {
          value = Utility.
              computePerSecondValue(timestamp, timestamps[d - 1], value,
                  dataValue[d - 1], samplePeriod);
          sample.add(timestamp, value);
        }
      }
    } else {
      for (int d = 0; d < timestamps.length; d++) {
        sample.add(timestamps[d], dataValue[d]);
      }
    }

    return sample;
  }

	private void createChart(StatValue sv) {
    statsArray.add(sv);

    updateValuePanel(sv);
    if (!(sv.getSnapshotsAverage() == 0 &&
        sv.getSnapshotsMaximum() == 0 &&
        sv.getSnapshotsMinimum() == 0)) {

      XYSeries series = createXYSeries(sv);
      XYSeriesCollection xysc = new XYSeriesCollection(series);
      chart = ChartFactory.createTimeSeriesChart(null, null,
          sv.getDescriptor().getUnits(), xysc, true, true, true);

      XYPlot plot = chart.getXYPlot();
      plot.setRangePannable(true);
      plot.setDomainPannable(true);
      plot.setBackgroundPaint(PLOT_BACKGROUND);
      //      plot.setDrawingSupplier(new VSDDrawingSupplier());

      cp = new ChartPanel(chart);
      this.getContentPane().add(cp, BorderLayout.CENTER);
      cp.setMouseWheelEnabled(true);
      cp.setMouseZoomable(true);
      cp.addChartMouseListener(this);

      hud = new HUD();
      cp.addOverlay(new PanelOverlay(hud, cp));
    }
	}

	private void updateValuePanel(StatValue sv) {
//		valuePanel.setMax(sv.getSnapshotsMaximum());
//		valuePanel.setMin(sv.getSnapshotsMinimum());
//		valuePanel.setStdDev(sv.getSnapshotsStandardDeviation());
//		valuePanel.setMean(sv.getSnapshotsAverage());
//		valuePanel.setDescription(sv.getDescriptor().getDescription());
//		valuePanel.setStatName(getIndexedStatName(sv));
//		valuePanel.setUnits(sv.getDescriptor().getUnits());
//		valuePanel.setSamples(sv.getSnapshotsSize());
//		valuePanel.updateValues();
	}

	private String getIndexedStatName(StatValue sv) {
		int index = statsArray.indexOf(sv);
		String statname = Integer.toString(index) + "_" + sv.getDescriptor().
						getName();
		return statname;
	}

	private StatValue lookupIndexedStatName(String name) {
		int charIndex = name.indexOf("_");
		int svIndex = Integer.parseInt(name.substring(0, charIndex));
		return statsArray.get(svIndex);
	}

	public void windowOpened(WindowEvent e) {
	}

	public void windowClosing(WindowEvent e) {
		mainWindow.removeChartWindow(this);
	}

	public void windowClosed(WindowEvent e) {
	}

	public void windowIconified(WindowEvent e) {
	}

	public void windowDeiconified(WindowEvent e) {
	}

	public void windowActivated(WindowEvent e) {
	}

	public void windowDeactivated(WindowEvent e) {
	}

	public void addToChart(List<StatValue> stats) {
    for (StatValue sv : stats) {
      statsArray.add(sv);

      if (!(sv.getSnapshotsAverage() == 0 &&
          sv.getSnapshotsMaximum() == 0 &&
          sv.getSnapshotsMinimum() == 0)) {

        XYSeries series = createXYSeries(sv);
        XYItemRenderer renderer2 = new StandardXYItemRenderer();
        XYPlot plot = chart.getXYPlot();
        NumberAxis currentAxis = null;
        boolean foundAxis = false;
        int axisnum = 0;
        for (axisnum = 0; axisnum < plot.getRangeAxisCount(); axisnum++) {
          currentAxis = (NumberAxis) plot.getRangeAxis(axisnum);
          if (currentAxis.getLabel().equals(sv.getDescriptor().getUnits())) {
            foundAxis = true;
            break;
          }

        }
        if (!foundAxis) {
          NumberAxis numAxis = new NumberAxis(sv.getDescriptor().getUnits());
          numAxis.setAutoRange(true);
          numAxis.setRange(0, sv.getSnapshotsMaximum() * 1.1);
          plot.setRangeAxis(plotNumber.get(), numAxis);
          plot.setDataset(plotNumber.get(), new XYSeriesCollection(series));
          plot.setRangeAxisLocation(plotNumber.get(),
              AxisLocation.BOTTOM_OR_LEFT);
          plot.mapDatasetToRangeAxis(plotNumber.get(), plotNumber.get());
          plot.setRenderer(plotNumber.get(), renderer2);
          plotNumber.incrementAndGet();
        } else {

          currentAxis.setAutoRange(true);
          if ((sv.getSnapshotsMaximum() * 1.1) > currentAxis.getRange().
              getUpperBound()) {
            currentAxis.setRange(0, sv.getSnapshotsMaximum() * 1.1);
          }
          XYSeriesCollection a = (XYSeriesCollection) plot.getDataset(axisnum);
          a.addSeries(series);

          plot.setRangeAxis(axisnum, currentAxis);
          // plot.setDataset(priorPlotnumber, xysc);
          plot.mapDatasetToRangeAxis(axisnum, axisnum);
          // plot.setRenderer(priorPlotnumber, renderer2);
        }
        renderer2.setBaseToolTipGenerator(new XYToolTipGenerator() {

          public String generateToolTip(XYDataset dataset, int series,
              int item) {
            Number x = dataset.getX(series, item);
            Number y = dataset.getY(series, item);
            String yf = String.format("%.2f", y);
            Date date = new Date(x.longValue());
            DateFormat df = DateFormat.
                getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

            String tooltip = df.format(date) + "," + yf;
            return tooltip;
          }
        });
        cp.setDisplayToolTips(true);
        updateValuePanel(sv);
      }
    }
  }

  @Override
  public void chartMouseClicked(ChartMouseEvent cme) {
    if (cme.getEntity() instanceof LegendItemEntity) {
      LegendItemEntity lie = (LegendItemEntity) cme.getEntity();
      XYPlot plot = cme.getChart().getXYPlot();

      // Reset the previously selected...
      if (lastDataset != null) {
        int idx = lastDataset.getSeriesIndex(lastSeries);
        plot.getRendererForDataset(lastDataset).setSeriesPaint(idx, lastPaint);
      }

      // Did we just re-select our last one?
      if (lastDataset == lie.getDataset()) {
        lastDataset = null;
        return;
      }

      lastSeries = lie.getSeriesKey();
      lastDataset = (XYSeriesCollection) lie.getDataset();
      int idx = lastDataset.getSeriesIndex(lastSeries);

      XYItemRenderer r = plot.getRendererForDataset(lastDataset);
      lastPaint = r.getSeriesPaint(idx);
      r.setSeriesPaint(idx, SELECTED_SERIES);
    }
  }

  @Override
  public void chartMouseMoved(ChartMouseEvent cme) {
    int viewX = cme.getTrigger().getX();

    XYPlot plot = (XYPlot) chart.getPlot();
    ValueAxis xAxis = plot.getDomainAxis();
    Rectangle2D dataArea = cp.getScreenDataArea();
    RectangleEdge xAxisEdge = plot.getDomainAxisEdge();

    double domainX = 0.0;
    double domainY = 0.0;
    double xx = xAxis.java2DToValue(viewX, dataArea, xAxisEdge);
    for (int i = 0; i < plot.getDataset().getItemCount(0); i++) {
      double dataX = plot.getDataset().getXValue(0, i);
      if (dataX >= xx) {
        domainX = dataX;
        domainY = plot.getDataset().getYValue(0, i);
        hud.setXLabel(X_AXIS_DATE_FORMAT.format(new Date((long) domainX)));
            hud.setYLabel(Double.toString(domainY));
        break;
      }
    }

    setCrosshairLocation(domainX, domainY);
  }

  protected void setCrosshairLocation(double x, double y) {
    Crosshair domainCrosshair;
    Crosshair rangeCrosshair;

    if (crosshairOverlay == null) {
      crosshairOverlay = new CrosshairOverlay();
      domainCrosshair = new Crosshair();
      domainCrosshair.setPaint(ChartColor.LIGHT_BLUE);
      domainCrosshair.setStroke(dashed);
      domainCrosshair.setVisible(true);
      crosshairOverlay.addDomainCrosshair(domainCrosshair);

      cp.addOverlay(crosshairOverlay);
    } else {
      domainCrosshair = (Crosshair) crosshairOverlay.getDomainCrosshairs().get(0);
    }

    domainCrosshair.setValue(x);
  }

  @Override
  public void keyTyped(KeyEvent e) {
  }

  @Override
  public void keyPressed(KeyEvent e) {
  }

  @Override
  public void keyReleased(KeyEvent e) {
    if ((e.getKeyCode() == KeyEvent.VK_DELETE ||
        e.getKeyCode() == KeyEvent.VK_BACK_SPACE) &&
        lastDataset != null) {

//      chart.getXYPlot().getDataset()
    }
  }
}
