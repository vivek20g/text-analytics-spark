package com.easyjet.test.spark.mllib;

//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.web.support.SpringBootServletInitializer;
//import org.springframework.boot.builder.SpringApplicationBuilder;
//import org.springframework.web.bind.annotation.PathVariable;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
//public class Application extends SpringBootServletInitializer {
public class Application {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Main Called");
		SpringApplication.run(Application.class, args);
	}
	
	
    /*protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
    	System.out.println("configure Called");
        return application.sources(Application.class);
    }*/

}
/*
@RestController

class HelloController {
	@Autowired
	TestSparkMLLib testSparkMLLib;
	
    @RequestMapping("/hello/{name}")
    
    String hello(@PathVariable String name) {
    	
    	System.out.println("Vivek Here");
    	String[] args = new String[2];
    	testSparkMLLib.startKafka(args);
    	
        return "Hi " + name + " !";
 
    }
}*/