package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;


/**
 * Clase con los test unitarios para comprobar el funcionamiento de la herramienta.
 */
public class TestApp
{
	 private JavaSparkContext sc = null;
		
		@Before
		public void setUp() {
			
			SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestApp");
			sc = new JavaSparkContext(conf);
		}
		
		@After
		public void setDown() {
			sc.close();
		}
		
		@Test
		public void testSuccessReduceInteger() {
			
			JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		    
		    JavaRDDTest testRDD = new JavaRDDTest(rdd);
		    
		    boolean test = testRDD.reduceTestInteger((x,y) -> x*y);
		    
		    System.out.println(test);
			
		}
				
		@Test
		public void testFailInteger() {
			
			JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		    
		    JavaRDDTest testRDD = new JavaRDDTest(rdd);
		    
		    boolean test = testRDD.reduceTestInteger((x,y) -> x-y);
		    
		    System.out.println(test);
			
		}
		
		@Test
		public void testSuccessReduceDouble() {
			
			JavaRDD<Double> rdd = sc.parallelize(Arrays.asList(1.0,2.1,3.2,4.3,5.1,6.2,7.9,8.1,0.1,10.1));
		    
		    JavaRDDTest testRDD = new JavaRDDTest(rdd);
		    
		    boolean test = testRDD.reduceTestDouble((x,y) -> x+y);
		    
		    System.out.println(test);
			
		}
		
		@Test
		public void testFailReduceDouble() {
			
			JavaRDD<Double> rdd = sc.parallelize(Arrays.asList(1.0,2.1,3.2,4.3,5.1,6.2,7.9,8.1,0.1,10.1));
		    
		    JavaRDDTest testRDD = new JavaRDDTest(rdd);
		    
		    boolean test = testRDD.reduceTestDouble((x,y) -> x/y);
		    
		    System.out.println(test);
			
		}
		
		@Test
		public void testFailReduceString() {
			
		    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a","b","c","d","f","g","h","i"));
				    
			JavaRDDTest testRDD = new JavaRDDTest(rdd);
			
			boolean test = testRDD.reduceTestString((x,y) -> new StringBuilder().append(x).append(y).toString());
			
			System.out.println(test);
			 	
		}
		
		@Test
		public void testSuccessReduceByKey() {
			
			List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String,Integer>>();
			
			list.add(new Tuple2<String, Integer>("A", 3));
			list.add(new Tuple2<String, Integer>("A", 1));
			list.add(new Tuple2<String, Integer>("B", 2));
			list.add(new Tuple2<String, Integer>("C", 7));
			
			JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
			
			JavaPairRDD<String, Integer> rdd2 = rdd.reduceByKey((x,y) -> x+y);
			
			System.out.println(rdd2.collect());
			
		}
}
