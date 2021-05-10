package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

/**
 * Clase con los test unitarios para comprobar el funcionamiento de la
 * herramienta.
 */
public class TestApp {

	/**
	 * Spark Context
	 */
	private JavaSparkContext sc = null;

	/**
	 * Lista de Integers
	 */
	private List<Integer> integerList = null;
	
	/**
	 * Lista de Doubles
	 */
	private List<Double> doubleList = null;
	
	/**
	 * Lista de frases (String)
	 */
	private List<String> linesList = null;
	
	/**
	 * Lista de palabras (String)
	 */
	@SuppressWarnings("unused")
	private List<String> wordsList = null;
	
	/**
	 * Lista de letras (String)
	 */
	private List<String> lettersList = null;
	
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
		integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		
		doubleList = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
		
		linesList = Arrays.asList("En un lugar de", "la mancha", "de cuyo nombre", "no quiero acordarme");
		
		wordsList = Arrays.asList("no", "ha", "mucho", "tiempo", "que", "vivia", "un", "hidalgo", "de", "los", "de", "lanza", "en", "astillero");
		
		lettersList = Arrays.asList("a", "d", "a", "r", "g", "a", "a", "n", "t", "i", "g", "u", "a");
		
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
	 * Test para comprobar el correcto funcionamiento de una funcion <b>reduce()</b> de tipo <i>Integer</i>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessReduceInteger() {

		JavaRDD<Integer> rdd = sc.parallelize(integerList);

		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);

		// Sumar los elementos
		rddTest.reduce((x, y) -> x + y);
		
		// Multiplicar los elementos
		rddTest.reduce((x, y) ->  x * y);

	}

	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduce()</b> de tipo <i>Integer</i>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduceInteger() {

		JavaRDD<Integer> rdd = sc.parallelize(integerList);

		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);

		// Restar los elementos
		rddTest.reduce((x, y) -> x - y);
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>reduce()</b> de tipo <i>Double</i>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessReduceDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);
		
		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		// Sumar los elementos
		rddTest.reduce((x, y) -> x + y);
		
		// Multiplicar los elementos
		rddTest.reduce((x, y) ->  x * y);
	}
	
	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduce()</b> de tipo <i>Double</i>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduceDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);
		
		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.reduce((x, y) -> x - y);
	}

	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduce()</b> de tipo <i>String</i>.
	 * Se usa una concatenacion de letras.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduceString() {

		JavaRDD<String> rdd = sc.parallelize(lettersList);

		JavaRDDTest<String> testRDD = new JavaRDDTest<>(rdd);

		testRDD.reduce((x, y) -> new StringBuilder().append(x).append(y).toString());
	}

	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>reduceByKey()</b> de tipo <i>Integer</i>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessReduceByKey() {
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tupleList);

		JavaPairRDDTest<String, Integer> testPairRDD = new JavaPairRDDTest<>(rdd);

		// Sumar los elementos
		testPairRDD.reduceByKey((x, y) -> x + y);
		
		// Multiplicar los elementos
		testPairRDD.reduceByKey((x, y) -> x * y);
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
	 * Test para comprobar el correcto funcionamiento de una funcion <b>map()</b> de tipo <i>String</i>.
	 * Se usa funcion para convertir a mayuscula la cadena de String.
	 */
	@Test
	public void testSuccessMapString() {
		
		JavaRDD<String> rdd = sc.parallelize(linesList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.map(s -> s.toUpperCase());
		
		rddTest.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
	}
	
}
