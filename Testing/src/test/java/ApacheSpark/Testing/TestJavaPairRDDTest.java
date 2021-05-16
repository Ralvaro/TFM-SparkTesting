package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import scala.Tuple3;

public class TestJavaPairRDDTest {
	
	/**
	 * Spark Context
	 */
	private JavaSparkContext sc = null;
	
	/**
	 * Lista de tuplas
	 */
	private List<Tuple2<String, Integer>> tupleList = null;

	/**
	 * Metodo para inicializar las variables y configurar y crear el Spark Context antes de ejecutar los tests.
	 */
	@Before
	public void setUp() {

		// Configurar el Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestApp");
		
		// Crear el Spark Context
		sc = new JavaSparkContext(conf);

		// Inicializar variables que se usaran en los test
		
		tupleList = new ArrayList<Tuple2<String, Integer>>();
			tupleList.add(new Tuple2<String, Integer>("C", 7));
			tupleList.add(new Tuple2<String, Integer>("A", 3));
			tupleList.add(new Tuple2<String, Integer>("A", 1));
			tupleList.add(new Tuple2<String, Integer>("B", 2));
			tupleList.add(new Tuple2<String, Integer>("C", 7));
			tupleList.add(new Tuple2<String, Integer>("A", 1));
			tupleList.add(new Tuple2<String, Integer>("B", 2));
			tupleList.add(new Tuple2<String, Integer>("C", 7));
			tupleList.add(new Tuple2<String, Integer>("A", 3));
			tupleList.add(new Tuple2<String, Integer>("A", 3));
			tupleList.add(new Tuple2<String, Integer>("A", 1));
			tupleList.add(new Tuple2<String, Integer>("B", 2));
			tupleList.add(new Tuple2<String, Integer>("C", 7));
		
		
	}

	/**
	 * Metodo para cerrar el Spark Context despues de ejecutar los tests.
	 */
	@After
	public void setDown() {
		
		// Cerrar el SparkContext
		sc.close();
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>reduceByKey()</b> de tipo <i>Integer</i>.
	 * Se usa una suma.
	 */
	@Test
	public void testSuccessReduceByKey() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);

		// Sumar los elementos
		testPairRDD.reduceByKey((x, y) -> x + y);
		
		// Indicando el numero de particiones
		testPairRDD.reduceByKey((x, y) -> x + y, rdd.getNumPartitions());
	}
	
	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduceByKey()</b> de tipo <i>Integer</i>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduceByKey() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);

		testPairRDD.reduceByKey((x, y) -> x - y);
	}
	
	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduceByKey()</b> de tipo <i>Integer</i>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduceByKey2() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);

		// Indicando el numero de particiones
		testPairRDD.reduceByKey((x, y) -> x - y, rdd.getNumPartitions());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>aggregateByKey()</b>.
	 * Se usa una funcion para calcular la media y desviacion tipica.
	 */
	@Test
	public void testSuccessAggregateByKey() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);
		
		testPairRDD
			.aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x), (acc1, acc2) -> acc1.merge(acc2))
			.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()));
		
		// Indicando el numero de particiones
		testPairRDD
			.aggregateByKey(new StatCounter(), rdd.getNumPartitions(), (acc, x) -> acc.merge(x), (acc1, acc2) -> acc1.merge(acc2))
			.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()));
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>aggregateByKey()</b>.
	 * Se usa una funcion para calcular la media y desviacion tipica añadiendo ruido aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailAggregateByKey() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);
		
		testPairRDD
			.aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x + new Random().nextFloat()), (acc1, acc2) -> acc1.merge(acc2))
			.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()));
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>aggregateByKey()</b>.
	 * Se usa una funcion para calcular la media y desviacion tipica añadiendo ruido aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailAggregateByKey2() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);
		
		// Indicando el numero de particiones
		testPairRDD
			.aggregateByKey(new StatCounter(), rdd.getNumPartitions(), (acc, x) -> acc.merge(x + new Random().nextFloat()), (acc1, acc2) -> acc1.merge(acc2))
			.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()));
		
	}

}
