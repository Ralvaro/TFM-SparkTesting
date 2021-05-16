package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class TestJavaDoubleRDDTest {
	
	/**
	 * Spark Context
	 */
	private JavaSparkContext sc = null;
	
	/**
	 * Lista de Doubles
	 */
	private List<Double> doubleList = null;

	/**
	 * Metodo para inicializar las variables y configurar y crear el Spark Context antes de ejecutar los tests.
	 */
	@Before
	public void setUp() {

		// Configurar el Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestApp");
		
		// Crear el Spark Context
		sc = new JavaSparkContext(conf);
		
		doubleList = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
		
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
	 * Test para comprobar el correcto funcionamiento de una funcion <b>reduce()</b>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessReduce() {

		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);

		// Sumar los elementos
		rddTest.reduce((x, y) -> x + y);
		
		// Multiplicar los elementos
		rddTest.reduce((x, y) ->  x * y);

	}

	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>reduce()</b>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailReduce() {

		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);

		// Restar los elementos
		rddTest.reduce((x, y) -> x - y);
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>treeReduce()</b>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessTreeReduce() {

		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);

		// Sumar los elementos
		rddTest.treeReduce((x, y) -> x + y);
		
		// Multiplicar los elementos
		rddTest.treeReduce((x, y) ->  x * y);
	}

	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>treeReduce()</b>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailTreeReduce() {

		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);

		// Restar los elementos
		rddTest.treeReduce((x, y) -> x - y);
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>map()</b>
	 * Se usa funcion para multiplicar los numeros por 3.
	 */
	@Test
	public void testSuccessMap() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.map(n -> n*3);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>map()</b>
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMap() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.map(n -> n*new Random().nextDouble());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>filter()</b>.
	 * Se usa funcion para filtrar los numeros pares de una lista de enteros.
	 */
	@Test
	public void testSuccessFilter() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.filter(n -> (n % 2) == 0);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>filter()</b>.
	 * Se usa funcion para filtrar los numeros de manera aleatoria.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFilter() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.filter(n -> new Random().nextBoolean());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMap()</b>.
	 * Se usa funcion para multiplicar por 3.
	 */
	@Test
	public void testSuccessFlatMap() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMap(n -> Arrays.asList(n* 3).iterator());
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>flatMap()</b>.
	 * Se usa funcion para multiplicar por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMap() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMap(n -> Arrays.asList(n* new Random().nextDouble()).iterator());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMapToPair()</b>.
	 * Se usa funcion para que por cada numero se cree una tupla de (numero, numero * (1,2,3,4,5,6)).
	 */
	@Test
	public void testSuccessFlatMapToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMapToPair(s -> {
			  List<Tuple2<Double, Double>> pairs2 = new LinkedList<>();
			  for (Integer n : Arrays.asList(1,2,3,4,5,6)) {
				  pairs2.add(new Tuple2<>(s, s*n));
			  }
			  return pairs2.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMapToPair()</b>.
	 * Se usa funcion para que por cada numero se cree una tupla de (numero, numero * (1,2,3,4,5,6)).
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMapToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMapToPair(s -> {
			  List<Tuple2<Double, Double>> pairs2 = new LinkedList<>();
			  for (Integer n : Arrays.asList(1,2,3,4,5,6)) {
				  pairs2.add(new Tuple2<>(s, n*new Random().nextDouble()));
			  }
			  return pairs2.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMapToDouble()</b>.
	 * Se usa funcion para multiplicar el elemento por si mismo.
	 */
	@Test
	public void testSuccessFlatMapToDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMapToDouble(s -> Arrays.asList(s*s).iterator());
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>flatMapToDouble()</b>.
	 * Se usa funcion para multiplicar el elemento por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMapToDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.flatMapToDouble(s -> Arrays.asList(s*(new Random().nextDouble())).iterator());
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapToPair()</b>.
	 * Se usa funcion para generar tuplas de un (numero, 2* numero).
	 */
	@Test
	public void testSuccessMapToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapToPair(n -> new Tuple2<Double, Double>(n, 2*n));
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapToPair()</b>.
	 * Se usa funcion para generar tuplas de un (numero, 2* numuero aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapToPair(n -> new Tuple2<Double, Double>(n, 2 * new Random().nextDouble()));
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitions() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitions((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitions((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitions() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitions((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitions2() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitions((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		}, true);
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitionsToDouble()</b>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitionsDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitionsToDouble((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		}, true);
	}

	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsToDouble()</b>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsDouble() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsToDouble()</b>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsDouble2() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Double> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		}, true);
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion que genera tuplas de (numero, numero*2).
	 */
	@Test
	public void testSuccessMapPartitionsToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Double> iter) -> {
		    ArrayList<Tuple2<Double, Double>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(new Tuple2<Double, Double>(current, current*2));
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitionsToPair((Iterator<Double> iter) -> {
		    ArrayList<Tuple2<Double, Double>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(new Tuple2<Double, Double>(current, current*2));
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion que genera tuplas de (numero, numero*aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsToPair() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Double> iter) -> {
		    ArrayList<Tuple2<Double, Double>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(new Tuple2<Double, Double>(current, current* new Random().nextDouble()));
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b>.
	 * Se usa funcion que genera tuplas de (numero, numero*aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsToPair2() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Double> iter) -> {
		    ArrayList<Tuple2<Double, Double>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(new Tuple2<Double, Double>(current, current* new Random().nextDouble()));
		    }
		    return out.iterator();
		}, true);
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitionsWithIndex()</b>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitionsWithIndex() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsWithIndex((index, iter) -> {
			ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * 2);
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsWithIndex()</b>.
	 * Se usa funcion para multiplicar los elementos por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsWithIndex() {
		
		JavaDoubleRDD rdd = sc.parallelizeDoubles(doubleList);

		JavaDoubleRDDTest rddTest = new JavaDoubleRDDTest(rdd);
		
		rddTest.mapPartitionsWithIndex((index, iter) -> {
			ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Double current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		}, true);
	}

}
