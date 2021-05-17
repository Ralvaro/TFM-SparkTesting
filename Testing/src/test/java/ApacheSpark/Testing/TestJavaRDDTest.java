package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class TestJavaRDDTest {
	
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
	private List<String> wordsList = null;
	
	/**
	 * Lista de letras (String)
	 */
	private List<String> lettersList = null;

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
	 * Test para comprobar el correcto funcionamiento de una funcion <b>treeReduce()</b> de tipo <i>Integer</i>.
	 * Se usa una suma y una multiplicacion.
	 */
	@Test
	public void testSuccessTreeReduceInteger() {

		JavaRDD<Integer> rdd = sc.parallelize(integerList);

		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);

		// Sumar los elementos
		rddTest.treeReduce((x, y) -> x + y);
		
		// Multiplicar los elementos
		rddTest.treeReduce((x, y) ->  x * y);
	}

	/**
	 * Test para comprobar el mal funcionamiento de una funcion <b>treeReduce()</b> de tipo <i>Integer</i>.
	 * Se usa una resta.
	 */
	@Test(expected=AssertionError.class)
	public void testFailTreeReduceInteger() {

		JavaRDD<Integer> rdd = sc.parallelize(integerList);

		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);

		// Restar los elementos
		rddTest.treeReduce((x, y) -> x - y);
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
	 * Test para comprobar el correcto funcionamiento de una funcion <b>map()</b> de tipo <i>String</i>.
	 * Se usa funcion para convertir a mayuscula la cadena de String.
	 */
	@Test
	public void testSuccessMapString() {
		
		JavaRDD<String> rdd = sc.parallelize(linesList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.map(s -> s.toUpperCase());
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>map()</b> de tipo <i>String</i>.
	 * Se usa funcion para agregar un numero aleatorio al final de cada palabra.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapString() {
		
		JavaRDD<String> rdd = sc.parallelize(wordsList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.map(s -> new StringBuilder().append(s).append(new Random().nextInt()).toString());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>filter()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para filtrar los numeros pares de una lista de enteros.
	 */
	@Test
	public void testSuccessFilterInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.filter(n -> (n % 2) == 0);
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>filter()</b> de tipo <i>String</i>.
	 * Se usa funcion para filtrar las palabras que contienen la letra 'm'.
	 */
	@Test
	public void testSuccessFilterString() {
		
		JavaRDD<String> rdd = sc.parallelize(wordsList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.filter(w -> w.contains("m"));
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>filter()</b> de tipo <i>Integer</i>.
	 * Se usa funcion escoger numeros aleatorios del conjunto.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFilterInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.filter(n -> new Random().nextBoolean());
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMap()</b> de tipo <i>String</i>.
	 * Se usa funcion para dividir las lineas en palabras.
	 */
	@Test
	public void testSuccessFlatMapString() {
		
		JavaRDD<String> rdd = sc.parallelize(linesList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>flatMap()</b> de tipo <i>String</i>.
	 * Se usa funcion para agregar un numero aleatorio al final de cada palabra y se dividen las palabras.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMapString() {
		
		JavaRDD<String> rdd = sc.parallelize(wordsList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMap(s -> Arrays.asList(new StringBuilder()
											.append(s)
											.append(new Random().nextInt()).toString().split(" ")).iterator());
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMapToPair()</b> de tipo <i>String</i>.
	 * Se usa funcion para dividir las lineas en palabras y generar un par (palabra, longitud de la palabra).
	 */
	@Test
	public void testSuccessFlatMapToPairString() {
		
		JavaRDD<String> rdd = sc.parallelize(linesList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMapToPair(s -> {
			  List<Tuple2<String, Integer>> pairs2 = new LinkedList<>();
			  for (String word : s.split(" ")) {
				  pairs2.add(new Tuple2<>(word, word.length()));
			  }
			  return pairs2.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>flatMapToPair()</b> de tipo <i>String</i>.
	 * Se usa funcion para dividir las lineas en palabras y generar un par (palabra, numero aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMapToPairString() {
		
		JavaRDD<String> rdd = sc.parallelize(linesList);
		
		JavaRDDTest<String> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMapToPair(s -> {
			  List<Tuple2<String, Integer>> pairs2 = new LinkedList<>();
			  for (String word : s.split(" ")) {
				  pairs2.add(new Tuple2<>(word, new Random().nextInt()));
			  }
			  return pairs2.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>flatMapToDouble()</b> de tipo <i>Double</i>.
	 * Se usa funcion para multiplicar el elemento por si mismo.
	 */
	@Test
	public void testSuccessFlatMapToDouble() {
		
		JavaRDD<Double> rdd = sc.parallelize(doubleList);
		
		JavaRDDTest<Double> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMapToDouble(s -> Arrays.asList(s*s).iterator());
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>flatMapToDouble()</b> de tipo <i>Double</i>.
	 * Se usa funcion para multiplicar el elemento por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailFlatMapToDouble() {
		
		JavaRDD<Double> rdd = sc.parallelize(doubleList);
		
		JavaRDDTest<Double> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.flatMapToDouble(s -> Arrays.asList(s*(new Random().nextDouble())).iterator());
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapToPair()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para generar tuplas de un (numero, 2* numero).
	 */
	@Test
	public void testSuccessMapToPairInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapToPair(n -> new Tuple2<Integer, Integer>(n, 2*n));
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapToPair()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para generar tuplas de un (numero, 2* numuero aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapToPairInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapToPair(n -> new Tuple2<Integer, Integer>(n, 2 * new Random().nextInt()));
		
	}
	
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitionsInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitions((Iterator<Integer> iter) -> {
		    ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * 2);
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitions((Iterator<Integer> iter) -> {
		    ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * 2);
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitions((Iterator<Integer> iter) -> {
		    ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * new Random().nextInt());
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsInteger2() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitions((Iterator<Integer> iter) -> {
		    ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * new Random().nextInt());
		    }
		    return out.iterator();
		}, true);
		
	}

	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitionsToDouble()</b> de tipo <i>Double</i>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitionsDouble() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Integer> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitionsToDouble((Iterator<Integer> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * 2.0);
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsToDouble()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsDouble() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Integer> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsToDouble()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los numeros por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsDouble2() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToDouble((Iterator<Integer> iter) -> {
		    ArrayList<Double> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * new Random().nextDouble());
		    }
		    return out.iterator();
		}, true);
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion que genera tuplas de (numero, numero*2).
	 */
	@Test
	public void testSuccessMapPartitionsToPairInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Integer> iter) -> {
		    ArrayList<Tuple2<Integer, Integer>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(new Tuple2<Integer, Integer>(current, current*2));
		    }
		    return out.iterator();
		});
		
		rddTest.mapPartitionsToPair((Iterator<Integer> iter) -> {
		    ArrayList<Tuple2<Integer, Integer>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(new Tuple2<Integer, Integer>(current, current*2));
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion que genera tuplas de (numero, numero*aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsToPairInteger() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Integer> iter) -> {
		    ArrayList<Tuple2<Integer, Integer>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(new Tuple2<Integer, Integer>(current, current* new Random().nextInt()));
		    }
		    return out.iterator();
		});
		
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitions()</b> de tipo <i>Integer</i>.
	 * Se usa funcion que genera tuplas de (numero, numero*aleatorio).
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsToPairInteger2() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsToPair((Iterator<Integer> iter) -> {
		    ArrayList<Tuple2<Integer, Integer>> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(new Tuple2<Integer, Integer>(current, current* new Random().nextInt()));
		    }
		    return out.iterator();
		}, true);
		
	}
	
	/**
	 * Test para comprobar el correcto funcionamiento de una funcion <b>mapPartitionsWithIndex()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los elementos por 2.
	 */
	@Test
	public void testSuccessMapPartitionsWithIndex() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsWithIndex((index, iter) -> {
			ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * 2);
		    }
		    return out.iterator();
		}, true);
	}
	
	/**
	 * Test para comprobar el incorrecto funcionamiento de una funcion <b>mapPartitionsWithIndex()</b> de tipo <i>Integer</i>.
	 * Se usa funcion para multiplicar los elementos por un numero aleatorio.
	 */
	@Test(expected=AssertionError.class)
	public void testFailMapPartitionsWithIndex() {
		
		JavaRDD<Integer> rdd = sc.parallelize(integerList);
		
		JavaRDDTest<Integer> rddTest = new JavaRDDTest<>(rdd);
		
		rddTest.mapPartitionsWithIndex((index, iter) -> {
			ArrayList<Integer> out = new ArrayList<>();
		    while(iter.hasNext()) {
		    	Integer current = iter.next();
		        out.add(current * new Random().nextInt());
		    }
		    return out.iterator();
		}, true);
	}

}
