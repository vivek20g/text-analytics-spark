package com.easyjet.test.spark.mllib;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
//import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import scala.reflect.ClassTag;
import scala.Tuple2;

@Component
public class TestSparkMLLib implements CommandLineRunner, Serializable{
	
	/*
	@Autowired
	KafkaSender kafkaSender;
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	*/
	public void run(String... arg0) throws Exception {
		// TODO Auto-generated method stub
		startKafka(arg0);
	}
	
	public void startKafka(String... args){
		
		final SparkSession spark = SparkSession
				  .builder()
				  .appName("Spark_MLLib")
				  .config("spark.master", "local[*]")
				  .getOrCreate();
				  
		Logger.getLogger("org").setLevel(Level.OFF);//does not work
		
		//spark.sparkContext().setLogLevel("WARN");//does not work
		
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		//sc.setLogLevel("WARN");//does not work
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		
		Set<String> topics = Collections.singleton("first-topic");
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "172.17.0.8:9092");
		
		System.out.println("Created Spark Session");
		
		//System.setProperty("com.amazonaws.services.s3.enableV4", "true");
		//sc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
		//sc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true");
		//sc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com");
		//sc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
		//sc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		sc.hadoopConfiguration().set("fs.s3.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		sc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AKIAJHGXXG6YXL3HHZPQ");
		sc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "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");
		
		
		JavaRDD rdd1 = sc.textFile("s3://vivek-test-spark1/testData.txt")
				.map(new Function<String, TestData>() {
				    @Override
				    public TestData call(String line) throws Exception {
				      String[] parts = line.split(",");
				      TestData testData = new TestData();
				      testData.setClassification(parts[0]);
				      testData.setText(parts[1].trim());
				      return testData;
				    }
				  });
		System.out.println("Showing df1");
		Dataset<Row> df1 = spark.createDataFrame(rdd1, TestData.class);
		df1.show();
		
		System.out.println("Showed df1");
		
		//Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/testData.txt").toDF();
		Dataset<Row> df = spark.read().option("header", "true").csv("s3://vivek-test-spark1/testData.txt").toDF();
		//https://github.com/vivek20g/text-analytics-spark/blob/master/testData.txt
		//Dataset<Row> df = spark.read().option("header", "true").csv("testData.txt").toDF();
		//Dataset<Row> df = spark.read().option("header", "true").csv("classpath:testData.txt").toDF();
		
		df.show();
		
		final RegexTokenizer regexTokenizer = new RegexTokenizer()
        .setInputCol("Text")
        .setOutputCol("words")
        .setPattern("\\W"); 
		
		Dataset<Row> tokenized = regexTokenizer.transform(df);
		tokenized.show();
		
		final StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");

		Dataset<Row> stopWordsRemoved = remover.transform(tokenized);
		stopWordsRemoved.show();
		
		int numFeatures = 50;
		final HashingTF hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);
		
		Dataset<Row> rawFeaturizedData = hashingTF.transform(stopWordsRemoved);
		
		rawFeaturizedData.foreach(new ForeachFunction<Row>() {
			public void call(Row row) {
                System.out.println("TF "+row.toString()); 
			}
		});
	    
		rawFeaturizedData.show();
        
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        final IDFModel idfModel = idf.fit(rawFeaturizedData);

        Dataset<Row> featurizedData = idfModel.transform(rawFeaturizedData);
        featurizedData.show();
        
        featurizedData.foreach(new ForeachFunction<Row>() {
			public void call(Row row) {
                System.out.println("IDF "+row.toString()); 
			}
		});
		
        JavaRDD<LabeledPoint> labelledJavaRDD = featurizedData.select("Classification", "features").toJavaRDD()
        		.map(new Function<Row, LabeledPoint>() {
					public LabeledPoint call(Row t) {
						System.out.println("t.get(0)"+t.get(0));
						System.out.println("t.get(1)"+t.get(1));
						Vector vectorFeatures = null;
						if(t.get(1) instanceof org.apache.spark.ml.linalg.SparseVector){
							org.apache.spark.ml.linalg.SparseVector sparseVector = (org.apache.spark.ml.linalg.SparseVector)t.get(1);
							org.apache.spark.ml.linalg.DenseVector dense = sparseVector.toDense();
							vectorFeatures = org.apache.spark.mllib.linalg.Vectors.dense(dense.toArray());
						}
						else{
							vectorFeatures = (Vector)t.get(1);
						}
						
						LabeledPoint labeledPoint = new LabeledPoint(new Double(t.get(0).toString()).doubleValue(), vectorFeatures) ;
						
                        return labeledPoint;
						
					}
                });
        labelledJavaRDD.collect();
        NaiveBayes naiveBayes = new NaiveBayes(1.0, "multinomial");
        final NaiveBayesModel naiveBayesModel = naiveBayes.train((org.apache.spark.rdd.RDD)labelledJavaRDD.rdd(), 1.0);
        
        final String[] testKafka = new String[1];
        
        JavaPairInputDStream<String, String> directKafkaStream 
        					= KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        
                
        directKafkaStream.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
			
			public void call(JavaPairRDD<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("arg.count() "+arg0.count());
		
				ClassTag<org.apache.spark.sql.SparkSession> classTagSparkSession 
							= scala.reflect.ClassTag$.MODULE$.apply(org.apache.spark.sql.SparkSession.class);
				
				final org.apache.spark.broadcast.Broadcast<org.apache.spark.sql.SparkSession> broadcastSSC 
										= arg0.context().broadcast(spark,classTagSparkSession);
				
				arg0.foreach(new VoidFunction<Tuple2<String,String>>() {

					public void call(Tuple2<String, String> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						System.out.println("arg0._1"+arg0._1);
						System.out.println("arg0._2"+arg0._2);
						testKafka[0] = arg0._2;
						
						String testString = testKafka[0];
		
						List<String> testList = Arrays.asList(testString);
				        
				        Dataset<Row> dfTest = broadcastSSC.value().createDataset(testList, Encoders.STRING()).withColumnRenamed("value", "Text");
				        dfTest.show();
				        Dataset<Row> tokenizedTest = regexTokenizer.transform(dfTest);
				        tokenizedTest.show();
				        Dataset<Row> stopWordsRemovedTest = remover.transform(tokenizedTest);
						stopWordsRemovedTest.show();
						Dataset<Row> rawFeaturizedDataTest = hashingTF.transform(stopWordsRemovedTest);
						rawFeaturizedDataTest.show();
						Dataset<Row> featurizedDataTest = idfModel.transform(rawFeaturizedDataTest);
				        featurizedDataTest.show();
				        
				        JavaRDD<Vector> vectorJavaRDDTest = featurizedDataTest.select("features").toJavaRDD().map(new Function<Row, Vector>() {
				        	
							public Vector call(Row t) {
								System.out.println("t.get(0)"+t.get(0));
								
								Vector vectorFeatures = null;
								if(t.get(0) instanceof org.apache.spark.ml.linalg.SparseVector){
									org.apache.spark.ml.linalg.SparseVector sparseVector = (org.apache.spark.ml.linalg.SparseVector)t.get(0);
									org.apache.spark.ml.linalg.DenseVector dense = sparseVector.toDense();
									vectorFeatures = org.apache.spark.mllib.linalg.Vectors.dense(dense.toArray());
								}
								else{
									vectorFeatures = (Vector)t.get(0);
								}
								
								return vectorFeatures;
								
							}
				        });
				        //vectorJavaRDDTest.collect().get(0);
				        
				        double predictedLabel = naiveBayesModel.predict(vectorJavaRDDTest.collect().get(0));
				        
				        System.out.println("printing "+predictedLabel);
				        
				        String kafkaTopic = "admin2";
				        Map<String, Object> props = new HashMap();
				        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.17.0.8:9092");
				        //props.put("zookeeper.connect", "localhost:2181");
				        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory(props);
				        
				        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate(producerFactory);
				        kafkaTemplate.send(kafkaTopic, String.valueOf(predictedLabel));
				        
						
					}
				});
				
			}
		});
        
        //System.out.println("testKafka[0] "+testKafka[0]);
        
        ssc.start();
        try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
