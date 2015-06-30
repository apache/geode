package com.gemstone.databrowser.objects;

import hydra.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import objects.ConfigurableObject;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.databrowser.Testutil;

/**
 * positions ObjectType.
 *
 * @author vreddy
 *
 */
public class positions implements ConfigurableObject, Serializable,
    DataSerializable {

  private static List secIds;

  private static List countries;

  private static List issuers;

  private static List bondRatings;

  private static List mktValues;

  public String secId;

  private String country;

  private String issuer;

  private String bondRating;

  private double mktValue;

  private double delta;

  private short industry;

  private double sharesOutstanding;

  private float qty;

  private String secLinks;

  public String secType;

  private int pid;

  public static int cnt = 0;

  static {
    String sId = Testutil.getProperty("positions.secId");
    if (sId != null) {
      String[] allsecIds = sId.split(":");
      secIds = new ArrayList();
      for (int k = 0; k < allsecIds.length; k++) {
        secIds.add(allsecIds[k]);
      }
    }
    else {
      Log.getLogWriter().warning("positions.secId property is not available.");
    }

    String cntry = Testutil.getProperty("positions.country");
    if (cntry != null) {
      String[] allcountries = cntry.split(":");
      countries = new ArrayList();
      for (int l = 0; l < allcountries.length; l++) {
        countries.add(allcountries[l]);
      }
    }
    else {
      Log.getLogWriter()
          .warning("positions.country property is not available.");
    }

    String issur = Testutil.getProperty("positions.issuers");
    if (issur != null) {
      String[] allissuers = issur.split(":");
      issuers = new ArrayList();
      for (int m = 0; m < allissuers.length; m++) {
        issuers.add(allissuers[m]);
      }
    }
    else {
      Log.getLogWriter()
          .warning("positions.issuers property is not available.");
    }

    String bratings = Testutil.getProperty("positions.bondRating");
    if (bratings != null) {
      String[] allbondRatings = bratings.split(":");
      bondRatings = new ArrayList();
      for (int p = 0; p < allbondRatings.length; p++) {
        bondRatings.add(allbondRatings[p]);
      }
    }
    else {
      Log.getLogWriter().warning(
          "positions.bondRating property is not available.");
    }

    String mkvls = Testutil.getProperty("positions.mktValues");
    if (mkvls != null) {
      String[] allmktValues = mkvls.split(":");
      mktValues = new ArrayList();
      for (int q = 0; q < allmktValues.length; q++) {
        mktValues.add(Double.valueOf(allmktValues[q]));
      }
    }
    else {
      Log.getLogWriter().warning(
          "positions.mktValues property is not available.");
    }
  }

  /* public no-arg constructor required for DataSerializable */
  public positions() {
  }

  public positions(String id, double out) {
    secId = id;
    sharesOutstanding = out;
    pid = cnt++;
    this.mktValue = (double)cnt;
  }

  public boolean equals(Object o) {
    if (!(o instanceof positions))
      return false;
    return this.secId.equals(((positions)o).secId);
  }

  public int hashCode() {
    return this.secId.hashCode();
  }

  public static void resetCounter() {
    cnt = 0;
  }

  public double getMktValue() {
    return this.mktValue;
  }

  public String getSecId() {
    return secId;
  }

  public void setSecId(String secId) {
    this.secId = secId;
  }

  public String getIssuer() {
    return issuer;
  }

  public void setIssuer(String issuer) {
    this.issuer = issuer;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getSecLinks() {
    return secLinks;
  }

  public void setSecLinks(String secLinks) {
    this.secLinks = secLinks;
  }

  public String getBondRating() {
    return bondRating;
  }

  public void setBondRating(String bondRating) {
    this.bondRating = bondRating;
  }

  public double getDelta() {
    return delta;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }

  public short getIndustry() {
    return industry;
  }

  public void setIndustry(short industry) {
    this.industry = industry;
  }

  public void setMktValue(double mktValue) {
    this.mktValue = mktValue;
  }

  public float getQty() {
    return qty;
  }

  public void setQty(float qty) {
    this.qty = qty;
  }

  public String getSecType() {
    return secType;
  }

  public void setSecType(String secType) {
    this.secType = secType;
  }

  public int getPid() {
    return pid;
  }

  public void setPid(int pid) {
    this.pid = pid;
  }

  public double getSharesOutstanding() {
    return sharesOutstanding;
  }

  public void setSharesOutstanding(double sharesOutstanding) {
    this.sharesOutstanding = sharesOutstanding;
  }

  public String toString() {
    return "positions [secId=" + this.secId + " country=" + this.country
        + " issuer=" + this.issuer + " bondRating=" + this.bondRating
        + " mktValue=" + this.mktValue + " delta=" + this.delta + " industry="
        + this.industry + " sharesOutstanding=" + this.sharesOutstanding
        + " qty=" + this.qty + " secLinks=" + this.secLinks + " secType="
        + this.secType + " id=" + this.pid + "]";
  }

  public Set getSet(int size) {
    Set set = new HashSet();
    for (int i = 0; i < size; i++) {
      set.add("" + i);
    }
    return set;
  }

  public Set getCol() {
    Set set = new HashSet();
    for (int i = 0; i < 2; i++) {
      set.add("" + i);
    }
    return set;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.bondRating = DataSerializer.readString(in);
    this.country = DataSerializer.readString(in);
    this.delta = in.readDouble();
    this.industry = (short)in.readLong();
    this.issuer = DataSerializer.readString(in);
    this.mktValue = in.readDouble();
    this.qty = (float)in.readDouble();
    this.secId = DataSerializer.readString(in);
    this.secLinks = DataSerializer.readString(in);
    this.secType = DataSerializer.readString(in);
    this.sharesOutstanding = in.readDouble();
    this.pid = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.bondRating, out);
    DataSerializer.writeString(this.country, out);
    out.writeDouble(this.delta);
    out.writeLong(this.industry);
    DataSerializer.writeString(this.issuer, out);
    out.writeDouble(this.mktValue);
    out.writeDouble(this.qty);
    DataSerializer.writeString(this.secId, out);
    DataSerializer.writeString(this.secLinks, out);
    DataSerializer.writeString(this.secType, out);
    out.writeDouble(this.sharesOutstanding);
    out.writeInt(this.pid);
  }

  public int getIndex() {
    return pid;
  }

  public void validate(int index) {
    // Do Nothing.
  }

  public void init(int i) {

    String sos, dls, idstry, qy, pid;
    this.country = (String)countries.get((i) % countries.size());
    this.secId = (String)secIds.get((i) % secIds.size());
    this.issuer = (String)issuers.get((i) % issuers.size());
    this.bondRating = (String)bondRatings.get((i) % bondRatings.size());
    this.mktValue = ((Double)mktValues.get((i) % mktValues.size()))
        .doubleValue();
    this.secType = Testutil.getProperty("positions.secType");
    if (this.secType == null) {
      Log.getLogWriter()
          .warning("positions.secType property is not available.");
    }
    this.secLinks = Testutil.getProperty("positions.secLinks");
    if (this.secLinks == null) {
      Log.getLogWriter().warning(
          "positions.secLinks property is not available.");
    }

    sos = Testutil.getProperty("positions.sharesOutstanding");
    if (sos != null) {
      try {
        double dsos = Double.parseDouble(sos);
        this.sharesOutstanding = dsos;
      }
      catch (NumberFormatException e1) {
        Log.getLogWriter().error(e1);
      }
    }
    else {
      Log.getLogWriter().warning(
          "positions.sharesOutstanding property is not available.");
    }

    dls = Testutil.getProperty("positions.delta");
    if (dls != null) {
      try {
        double dl = Double.parseDouble(dls);
        this.delta = dl;
      }
      catch (NumberFormatException e2) {
        Log.getLogWriter().error(e2);
      }
    }
    else {
      Log.getLogWriter().warning("positions.delta property is not available.");
    }

    idstry = Testutil.getProperty("positions.industry");
    if (idstry != null) {
      try {
        short sh = Short.parseShort(idstry.trim());
        this.industry = sh;
      }
      catch (NumberFormatException e3) {
        Log.getLogWriter().error(e3);
      }
    }
    else {
      Log.getLogWriter().warning(
          "positions.industry property is not available.");
    }

    qy = Testutil.getProperty("positions.qty");
    if (qy != null) {
      float f;
      try {
        f = Float.parseFloat(qy);
        this.qty = f;
      }
      catch (NumberFormatException e4) {
        Log.getLogWriter().error(e4);
      }
    }
    else {
      Log.getLogWriter().warning("positions.qty property is not available.");
    }

    pid = Testutil.getProperty("positions.pid");
    if (pid != null) {
      try {
        int pd = Integer.parseInt(pid.trim());
        this.pid = pd;
      }
      catch (NumberFormatException nfe) {
        Log.getLogWriter().error(nfe);
      }
    }
    else {
      Log.getLogWriter().warning("positions.pid property is not available.");
    }
  }
}
