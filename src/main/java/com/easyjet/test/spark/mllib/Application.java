package com.easyjet.test.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class Application extends SpringBootServletInitializer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Vivek Main Called");
		SpringApplication.run(Application.class, args);
	}
	
	
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		System.out.println("Vivek configure Called");
        return application.sources(Application.class);
    }

}
