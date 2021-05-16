package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;

import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

/**
 * Clase que representa un objeto JavaRDDTest que contiene los metodos para testear las funciones que dispone un JavaRDD.
 * @author Alvaro R. Perez Entrenas
 */
public class JavaRDDTest<T> extends JavaRDD<T> {
	
	/**
	 * ID Serial por defecto.
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constante para determinar el numero maximo de particiones de un RDD (MAX_VALUE por defecto).
	 */
	private int maxNumPartitions = Integer.MAX_VALUE;

	/**
	 * Constante para determinar el numero de repetitiones.
	 */
	private int numRepetitions = 10;
	
	/**
	 * Objeto para obtener numeros aleatorios (12345 semilla por defecto).
	 */
	private Random rand = new Random(12345);
	
	/**
	 * Constructor dado un <b>RDD</b> y su <b>ClassTag</b>.
	 * @param rdd (RDD).
	 * @param classTag (ClassTagg).
	 */
	public JavaRDDTest(RDD<T> rdd, ClassTag<T> classTag) {
		super(rdd, classTag);
	}
	
	/**
	 * Constructor dado un JavaRDD.
	 * @param rdd (JavaRDD).
	 */
	public JavaRDDTest(JavaRDD<T> rdd) {
		super(rdd.rdd(), rdd.classTag());
	}
	
	/**
	 * Metodo para obtener el numero maximo de particiones permitidas.
	 * @return
	 */
	public int getMaxNumPartitions() {
		return maxNumPartitions;
	}

	/**
	 * Metodo para establecer el numero maximo de particiones permitidas.
	 * @param maxNumPartitions
	 */
	public void setMaxNumPartitions(int maxNumPartitions) {
		this.maxNumPartitions = maxNumPartitions;
	}
	
	/**
	 * Metodo para obtener el numero de repeticiones.
	 * @return numRepetitions
	 */
	public int getNumRepetitions() {
		return numRepetitions;
	}

	/**
	 * Metodo para establecer el numero de repeticiones.
	 * @param numRepetitions
	 */
	public void setNumRepetitions(int numRepetitions) {
		this.numRepetitions = numRepetitions;
	}

	/**
	 * Metodo para establecer la semilla de las funcionalidades aleatorias.
	 * @param seed
	 */
	public void setRand(long seed) {
		this.rand.setSeed(seed);
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduce()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param f (Function2<T, T, T>) - Funcion reduce que se desea testear.
	 * @return T - Resultado de la operacion.
	 */
	public T reduce(Function2<T, T, T> f) {
		
		T result = this.rdd().toJavaRDD().reduce(f);
		T resultToCompare = null;
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		resultToCompare = this.rdd().toJavaRDD().coalesce(1, false).reduce(f);
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = this.rdd().toJavaRDD().coalesce(maxNumPartitions, false).reduce(f);
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = this.rdd().toJavaRDD().reduce(f);
			Assert.assertEquals(result, resultToCompare);
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = this.rdd().toJavaRDD().coalesce(randomNumber, false).reduce(f);
			Assert.assertEquals(result, resultToCompare);
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = this.rdd().toJavaRDD().coalesce(this.rdd().getNumPartitions(), true).reduce(f);
			Assert.assertEquals(result, resultToCompare);
			
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>treeReduce()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param function (Function2<T, T, T>) - Funcion treeReduce que se desea testear.
	 * @return T - Resultado de la operacion.
	 */
	public T treeReduce(Function2<T, T, T> f) {
		
		T result = this.rdd().toJavaRDD().treeReduce(f);
		T resultToCompare = null;
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		resultToCompare = this.rdd().toJavaRDD().coalesce(1, false).treeReduce(f);
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = this.rdd().toJavaRDD().coalesce(maxNumPartitions, false).treeReduce(f);
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = this.rdd().toJavaRDD().treeReduce(f);
			Assert.assertEquals(result, resultToCompare);
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = this.rdd().toJavaRDD().coalesce(randomNumber, false).treeReduce(f);
			Assert.assertEquals(result, resultToCompare);
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = this.rdd().toJavaRDD().coalesce(this.rdd().getNumPartitions(), true).treeReduce(f);
			Assert.assertEquals(result, resultToCompare);
			
		}
		
		return result;
	}

	/**
	 * Metodo que comprueba si una funcion de tipo <b>filter()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (Function<T, Boolean>) - Funcion filter que se desea testear.
	 * @return JavaRDD<T> - Resultado de la operacion.
	 */
	public JavaRDD<T> filter(Function<T, Boolean> function) {
		
		JavaRDD<T> result = this.rdd().toJavaRDD().filter(function).sortBy(f -> f, true, this.getNumPartitions()).cache();
		
		// Idempotente
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<T> resultToCompare = this.rdd().toJavaRDD().filter(function).sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<T> randomPartitionRDDResult = randomPartitionRDD.filter(function);
		JavaRDD<T> subtracRDDResult = subtracRDD.filter(function);
		
		JavaRDD<T> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(f -> f, true, subtracRDD.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMap()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (FlatMapFunction<T, U>) - Funcion flatMap que se desea testear.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> flatMap(FlatMapFunction<T, U> function) {
		
		JavaRDD<U> result = this.rdd().toJavaRDD().flatMap(function).sortBy(f -> f, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.rdd().toJavaRDD().flatMap(function).sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<U> randomPartitionRDDResult = randomPartitionRDD.flatMap(function);
		JavaRDD<U> subtracRDDResult = subtracRDD.flatMap(function);
		
		JavaRDD<U> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(f -> f, true, subtracRDD.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMapToPair()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (PairFlatMapFunction<T, K2, V2>) - Funcion flatMapToPair que se desea testear.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> flatMapToPair(PairFlatMapFunction<T, K2, V2> f) {
		
		JavaPairRDD<K2, V2> result = this.rdd().toJavaRDD().flatMapToPair(f).sortByKey().cache();
		
		// Idempotente
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.rdd().toJavaRDD().flatMapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K2, V2> randomPartitionRDDResult = randomPartitionRDD.flatMapToPair(f).sortByKey();
		JavaPairRDD<K2, V2> subtracRDDResult = subtracRDD.flatMapToPair(f).sortByKey();
		
		JavaPairRDD<K2, V2> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortByKey();
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMapToPair()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (PairFlatMapFunction<T, K2, V2>) - Funcion flatMapToPair que se desea testear.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<T> f) {
		
		JavaDoubleRDD result = this.rdd().toJavaRDD().flatMapToDouble(f).cache();
		
		// Idempotente
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.rdd().toJavaRDD().flatMapToDouble(f);
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaDoubleRDD randomPartitionRDDResult = randomPartitionRDD.flatMapToDouble(f);
		JavaDoubleRDD subtracRDDResult = subtracRDD.flatMapToDouble(f);
		
		JavaDoubleRDD unionResult = randomPartitionRDDResult.union(subtracRDDResult);
		
		Assert.assertEquals(result.takeOrdered((int) result.count()), unionResult.takeOrdered((int) unionResult.count()));
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>map()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (Function<T, R>) - Funcion map que se desea testear.
	 * @return JavaRDD<R> - Resultado de la operacion.
	 */
	public <R> JavaRDD<R> map(Function<T, R> function) {
		
		JavaRDD<R> result = this.rdd().toJavaRDD().map(function);
		result = result.sortBy(f -> f, true, result.getNumPartitions());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = this.rdd().toJavaRDD().map(function);
			resultToCompare = resultToCompare.sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<R> randomPartitionRDDResult = randomPartitionRDD.map(function);
		JavaRDD<R> subtracRDDResult = subtracRDD.map(function);
		
		JavaRDD<R> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(f -> f, true, this.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapToPair()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (PairFunction<T, K, V>) - Funcion mapToPair que se desea testear.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public <K,V> JavaPairRDD<K, V> mapToPair(PairFunction<T, K, V> f) {
		
		JavaPairRDD<K, V> result = this.rdd().toJavaRDD().mapToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, V> resultToCompare = this.rdd().toJavaRDD().mapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K, V> randomPartitionRDDResult = randomPartitionRDD.mapToPair(f);
		JavaPairRDD<K, V> subtracRDDResult = subtracRDD.mapToPair(f);
		
		JavaPairRDD<K, V> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortByKey();
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitions()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param function (Function<Object, Object>) - Funcion mapPartitions que se desea testear.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>, U> function) {
		
		JavaRDD<U> result = this.rdd().toJavaRDD().mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.rdd().toJavaRDD().mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<U> randomPartitionRDDResult = randomPartitionRDD.mapPartitions(function);
		JavaRDD<U> subtracRDDResult = subtracRDD.mapPartitions(function);
		
		JavaRDD<U> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(f -> f, true, this.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitions()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (Function<Object, Object>) - Funcion mapPartitions que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>, U> f, boolean preservesPartitioning) {
		
		JavaRDD<U> result = this.rdd().toJavaRDD().mapPartitions(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.rdd().toJavaRDD().mapPartitions(f, preservesPartitioning);
			resultToCompare = resultToCompare.sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<U> randomPartitionRDDResult = randomPartitionRDD.mapPartitions(f, preservesPartitioning);
		JavaRDD<U> subtracRDDResult = subtracRDD.mapPartitions(f, preservesPartitioning);
		
		JavaRDD<U> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(e -> e, true, this.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToDouble()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (Function<Object, Object>) - Funcion mapPartitionsToDouble que se desea testear.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<T>> f) {
				
		JavaDoubleRDD result = this.rdd().toJavaRDD().mapPartitionsToDouble(f).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.rdd().toJavaRDD().mapPartitionsToDouble(f).cache();
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaDoubleRDD randomPartitionRDDResult = randomPartitionRDD.mapPartitionsToDouble(f);
		JavaDoubleRDD subtracRDDResult = subtracRDD.mapPartitionsToDouble(f);
		
		JavaDoubleRDD unionResult = randomPartitionRDDResult.union(subtracRDDResult);
		
		Assert.assertEquals(result.takeOrdered((int) result.count()), unionResult.takeOrdered((int) unionResult.count()));
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToDouble()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (Function<Object, Object>) - Funcion mapPartitionsToDouble que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<T>> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD result = this.rdd().toJavaRDD().mapPartitionsToDouble(f, preservesPartitioning).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.rdd().toJavaRDD().mapPartitionsToDouble(f, preservesPartitioning).cache();
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaDoubleRDD randomPartitionRDDResult = randomPartitionRDD.mapPartitionsToDouble(f, preservesPartitioning);
		JavaDoubleRDD subtracRDDResult = subtracRDD.mapPartitionsToDouble(f, preservesPartitioning);
		
		JavaDoubleRDD unionResult = randomPartitionRDDResult.union(subtracRDDResult);
		
		Assert.assertEquals(result.takeOrdered((int) result.count()), unionResult.takeOrdered((int) unionResult.count()));
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToPair()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (PairFlatMapFunction<Iterator<T>, K2, V2>) - Funcion mapPartitionsToPair que se desea testear.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<T>, K2, V2> f) {
		
		JavaPairRDD<K2, V2> result = this.rdd().toJavaRDD().mapPartitionsToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.rdd().toJavaRDD().mapPartitionsToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K2, V2> randomPartitionRDDResult = randomPartitionRDD.mapPartitionsToPair(f);
		JavaPairRDD<K2, V2> subtracRDDResult = subtracRDD.mapPartitionsToPair(f);
		
		JavaPairRDD<K2, V2> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortByKey();
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToPair()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (PairFlatMapFunction<Iterator<T>, K2, V2>) - Funcion mapPartitionsToPair que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<T>, K2, V2> f, boolean preservesPartitioning) {
		
		JavaPairRDD<K2, V2> result = this.rdd().toJavaRDD().mapPartitionsToPair(f, preservesPartitioning).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.rdd().toJavaRDD().mapPartitionsToPair(f, preservesPartitioning).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K2, V2> randomPartitionRDDResult = randomPartitionRDD.mapPartitionsToPair(f, preservesPartitioning);
		JavaPairRDD<K2, V2> subtracRDDResult = subtracRDD.mapPartitionsToPair(f, preservesPartitioning);
		
		JavaPairRDD<K2, V2> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortByKey();
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsWithIndex()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (Function2<Integer, Iterator<T>, Iterator<R>>) - Funcion mapPartitionsWithIndex que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaRDD<R> - Resultado de la operacion.
	 */
	public <R> JavaRDD<R> mapPartitionsWithIndex(Function2<Integer, Iterator<T>, Iterator<R>> f, boolean preservesPartitioning) {
		
		JavaRDD<R> result = this.rdd().toJavaRDD().mapPartitionsWithIndex(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = this.rdd().toJavaRDD().mapPartitionsWithIndex(f, preservesPartitioning);
			resultToCompare.sortBy(e -> e, true, resultToCompare.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<T> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<R> randomPartitionRDDResult = randomPartitionRDD.mapPartitionsWithIndex(f, preservesPartitioning);
		JavaRDD<R> subtracRDDResult = subtracRDD.mapPartitionsWithIndex(f, preservesPartitioning);
		
		JavaRDD<R> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(e -> e, true, result.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que crea un nuevo RDD de los elementos.
	 * @param rdd1
	 * @param rdd2
	 * @return
	 */
	private JavaRDD<T> subtractRDDElements(JavaRDD<T> rdd1, JavaRDD<T> rdd2) {
		
		ArrayList<T> collectRDD1 = new ArrayList<T>(rdd1.collect());
		ArrayList<T> collectRDD2 = new ArrayList<T>(rdd2.collect());
		
		for (T obj : collectRDD1) {
			collectRDD2.remove(obj);
		}
		
		int numPartitions = 1;
		if (rdd2.getNumPartitions() != 1) {
			numPartitions = rdd2.getNumPartitions() - 1;
		}
		
		return this.context().parallelize(JavaConversions.asScalaBuffer(collectRDD2), numPartitions, this.classTag()).toJavaRDD();
	}
	
	/**
	 * Metodo para obtener un RDD con los elementos de una particion aleatoria de otro RDD.
	 * @param rdd
	 * @return
	 */
	private JavaRDD<T> getRandomPartition(JavaRDD<T> rdd) {
		
		int randomPartition = (Math.abs(rand.nextInt()) % rdd().getNumPartitions());
		
		ArrayList<T> collectRDD = new ArrayList<T> (rdd.glom().collect().get(randomPartition));
		
		return this.context().parallelize(JavaConversions.asScalaBuffer(collectRDD), 1, this.classTag()).toJavaRDD();
	}

}
