package com.examples.gemfire.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.Instantiator;

public class Product implements DataSerializable {
	
    private int ProductID;
    private String Name;
    private String ProductNumber;
    private String MakeFlag;
    private String FinishedGoodsFlag;
    private String Color;
    private int SafetyStockLevel;
    private int ReorderPoint;
    private double StandardCost;
    private double ListPrice;
    private int DaysToManufacture;
    private String SellStartDate;
    private String DiscontinuedDate;

    static { // note that classID (7) must match C#
        Instantiator.register(new Instantiator(Product.class, (byte) 0x07) {
            public DataSerializable newInstance() {
            	return new Product();
        }
        });
    }

        
	public Product() {
		// TODO Auto-generated constructor stub
		
	}
	
	public Product(int prodId, String prodName, String prodNum, String makeFlag, String finished, String color,
            int safetyLock, int reorderPt, double stdCost, double listPrice, int mfgDays,
            String startDate, String discDate)
	{
		ProductID = prodId;
		Name = prodName;
		ProductNumber = prodNum;
		MakeFlag = makeFlag;
		FinishedGoodsFlag = finished;
		Color = color;
		SafetyStockLevel = safetyLock;
		ReorderPoint = reorderPt;
		StandardCost = stdCost;
		ListPrice = listPrice;
		DaysToManufacture = mfgDays;
		SellStartDate = startDate;
		DiscontinuedDate = discDate;
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException {

			ProductID = input.readInt();
			Name = input.readUTF();
			ProductNumber = input.readUTF();
			MakeFlag = input.readUTF();
			FinishedGoodsFlag = input.readUTF();
			Color = input.readUTF();
			SafetyStockLevel = input.readInt();
			ReorderPoint	= input.readInt();
			StandardCost = input.readDouble();
			ListPrice = input.readDouble();
			DaysToManufacture = input.readInt();
			SellStartDate = input.readUTF();
			DiscontinuedDate = input.readUTF();
	}

	public void toData(DataOutput output) throws IOException {
			output.writeInt(this.getProductID());
			output.writeUTF(this.getName());
			output.writeUTF(this.getProductNumber());
			output.writeUTF(this.getMakeFlag());
			output.writeUTF(this.getFinishedGoodsFlag());
			output.writeUTF(this.getColor());
			output.writeInt(this.getSafetyStockLevel());
	        output.writeInt(this.getReorderPoint());
	        output.writeDouble(this.getStandardCost());
	        output.writeDouble(this.getListPrice());
	        output.writeInt(this.getDaysToManufacture());
	        output.writeUTF(this.getSellStartDate());
	        output.writeUTF(this.getDiscontinuedDate());
	}

	public String getColor() {
		return Color;
	}

	public void setColor(String color) {
		Color = color;
	}

	public int getDaysToManufacture() {
		return DaysToManufacture;
	}

	public void setDaysToManufacture(int daysToManufacture) {
		DaysToManufacture = daysToManufacture;
	}

	public String getDiscontinuedDate() {
		return DiscontinuedDate;
	}

	public void setDiscontinuedDate(String discontinuedDate) {
		DiscontinuedDate = discontinuedDate;
	}

	public String getFinishedGoodsFlag() {
		return FinishedGoodsFlag;
	}

	public void setFinishedGoodsFlag(String finishedGoodsFlag) {
		FinishedGoodsFlag = finishedGoodsFlag;
	}

	public double getListPrice() {
		return ListPrice;
	}

	public void setListPrice(double listPrice) {
		ListPrice = listPrice;
	}

	public String getMakeFlag() {
		return MakeFlag;
	}

	public void setMakeFlag(String makeFlag) {
		MakeFlag = makeFlag;
	}

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public int getProductID() {
		return ProductID;
	}

	public void setProductID(int productID) {
		ProductID = productID;
	}

	public String getProductNumber() {
		return ProductNumber;
	}

	public void setProductNumber(String productNumber) {
		ProductNumber = productNumber;
	}

	public int getReorderPoint() {
		return ReorderPoint;
	}

	public void setReorderPoint(int reorderPoint) {
		ReorderPoint = reorderPoint;
	}

	public int getSafetyStockLevel() {
		return SafetyStockLevel;
	}

	public void setSafetyStockLevel(int safetyStockLevel) {
		SafetyStockLevel = safetyStockLevel;
	}

	public String getSellStartDate() {
		return SellStartDate;
	}

	public void setSellStartDate(String sellStartDate) {
		SellStartDate = sellStartDate;
	}

	public double getStandardCost() {
		return StandardCost;
	}

	public void setStandardCost(double standardCost) {
		StandardCost = standardCost;
	}

	public Class[] getSupportedClasses() {
		// TODO Auto-generated method stub
		return new Class[] {com.examples.gemfire.net.Product.class};
	}

}
