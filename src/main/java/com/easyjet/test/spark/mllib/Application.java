package com.easyjet.test.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);//does not work
		SpringApplication.run(Application.class, args);
	}

}
