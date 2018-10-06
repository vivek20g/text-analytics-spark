package com.easyjet.test.spark.mllib;

import java.io.Serializable;

public class TestData implements Serializable{
	
	private String Classification;
	private String Text;
	public String getClassification() {
		return Classification;
	}
	public void setClassification(String classification) {
		Classification = classification;
	}
	public String getText() {
		return Text;
	}
	public void setText(String text) {
		Text = text;
	}
	
}
