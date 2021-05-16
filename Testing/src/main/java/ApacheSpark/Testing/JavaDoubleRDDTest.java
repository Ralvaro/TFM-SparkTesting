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

/**
 * Clase que representa un objeto JavaDoubleRDDTest que contiene los metodos para testear las funciones que dispone un JavaDoubleRDD.
 * @author Alvaro R. Perez Entrenas
 */
public class JavaDoubleRDDTest extends JavaDoubleRDD {

	/**
	 * Serial Version ID por defecto.
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constante para determinar el numero maximo de particiones de un RDD (MAX_VALUE por defecto).
	 */
	private int maxNumPartitions = Integer.MAX_VALUE;
	
	/**
	 * Constante para determinar el numero de repeticiones (10 por defecto).
	 */
	private int numRepetitions = 10;
	
	/**
	 * Objeto para obtener numeros aleatorios (12345 semilla por defecto).
	 */
	private Random rand = new Random(12345);

	/**
	 * Constructor dado un RDD.
	 * @param srdd (rdd).
	 */
	public JavaDoubleRDDTest(RDD<Object> srdd) {
		super(srdd);
	}
	
	/**
	 * Constructor dado un JavaDoubleRDD.
	 * @param rdd (JavaDoubleRDD).
	 */
	public JavaDoubleRDDTest(JavaDoubleRDD rdd) {
		super(rdd.srdd());
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
	 * Metodo de test para una funcion <b>reduce()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param f (Function2<Double, Double, Double>) - Funcion reduce que se desea testear.
	 */
	public Double reduce(Function2<Double, Double, Double> f) {
		
		Double result = this.rdd().toJavaRDD().reduce(f);
		Double resultToCompare = null;
		
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
	 * @param function (Function2<Double, Double, Double>) - Funcion treeReduce que se desea testear.
	 * @return Double - Resultado de la operacion.
	 */
	public Double treeReduce(Function2<Double, Double, Double> f) {
		
		Double result = this.rdd().toJavaRDD().treeReduce(f);
		Double resultToCompare = null;
		
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
	 * @param function (Function<Double, Boolean>) - Funcion filter que se desea testear.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD filter(Function<Double, Boolean> function) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaDoubleRDD result = rdd.filter(function).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = rdd.filter(function);
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<Double> randomPartitionRDDResult = randomPartitionRDD.filter(function);
		JavaRDD<Double> subtracRDDResult = subtracRDD.filter(function);
		
		JavaRDD<Double> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(f -> f, true, subtracRDD.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMap()</b>.
	 *	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que falle un nodo.</li>
	 * 	</ul>
	 * @param f (FlatMapFunction<T, U>) - Funcion flatMap que se desea testear.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> flatMap(FlatMapFunction<Double, U> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaRDD<U> result = rdd.flatMap(f).sortBy(e -> e, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = rdd.flatMap(f).sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<U> randomPartitionRDDResult = randomPartitionRDD.flatMap(f);
		JavaRDD<U> subtracRDDResult = subtracRDD.flatMap(f);
		
		JavaRDD<U> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(e -> e, true, subtracRDD.getNumPartitions());
		
		Assert.assertEquals(result.collect(), unionResult.collect());
		
		return null;
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
	public <K2, V2> JavaPairRDD<K2, V2> flatMapToPair(PairFlatMapFunction<Double, K2, V2> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaPairRDD<K2, V2> result = rdd.flatMapToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = rdd.flatMapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K2, V2> randomPartitionRDDResult = randomPartitionRDD.flatMapToPair(f);
		JavaPairRDD<K2, V2> subtracRDDResult = subtracRDD.flatMapToPair(f);
		
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
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<Double> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaDoubleRDD result = rdd.flatMapToDouble(f).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = rdd.flatMapToDouble(f);
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	 * @param f (Function<T, R>) - Funcion map que se desea testear.
	 * @return JavaRDD<R> - Resultado de la operacion.
	 */
	public <R> JavaRDD<R> map(Function<Double, R> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaRDD<R> result = rdd.map(f);
		result = result.sortBy(e -> e, true, result.getNumPartitions());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = rdd.map(f);
			resultToCompare = resultToCompare.sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaRDD<R> randomPartitionRDDResult = randomPartitionRDD.map(f);
		JavaRDD<R> subtracRDDResult = subtracRDD.map(f);
		
		JavaRDD<R> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortBy(e -> e, true, this.getNumPartitions());
		
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
	public <K2,V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<Double, K2, V2> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaPairRDD<K2, V2> result = rdd.mapToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = rdd.mapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
		JavaPairRDD<K2, V2> randomPartitionRDDResult = randomPartitionRDD.mapToPair(f);
		JavaPairRDD<K2, V2> subtracRDDResult = subtracRDD.mapToPair(f);
		
		JavaPairRDD<K2, V2> unionResult = randomPartitionRDDResult.union(subtracRDDResult).sortByKey();
		
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
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<Double>, U> function) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaRDD<U> result = rdd.mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = rdd.mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<Double>, U> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaRDD<U> result = rdd.mapPartitions(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = rdd.mapPartitions(f, preservesPartitioning);
			resultToCompare = resultToCompare.sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Double>> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
				
		JavaDoubleRDD result = rdd.mapPartitionsToDouble(f).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = rdd.mapPartitionsToDouble(f).cache();
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Double>> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaDoubleRDD result = rdd.mapPartitionsToDouble(f, preservesPartitioning).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = rdd.mapPartitionsToDouble(f, preservesPartitioning).cache();
			Assert.assertEquals(result.takeOrdered((int) result.count()), resultToCompare.takeOrdered((int) resultToCompare.count()));
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Double>, K2, V2> f) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaPairRDD<K2, V2> result = rdd.mapPartitionsToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = rdd.mapPartitionsToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Double>, K2, V2> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaPairRDD<K2, V2> result = rdd.mapPartitionsToPair(f, preservesPartitioning).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = rdd.mapPartitionsToPair(f, preservesPartitioning).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	public <R> JavaRDD<R> mapPartitionsWithIndex(Function2<Integer, Iterator<Double>, Iterator<R>> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD rdd = new JavaDoubleRDD(this.srdd());
		
		JavaRDD<R> result = rdd.mapPartitionsWithIndex(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = rdd.mapPartitionsWithIndex(f, preservesPartitioning);
			resultToCompare.sortBy(e -> e, true, result.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Que un nodo aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Double> randomPartitionRDD = getRandomPartition(this.rdd().toJavaRDD());
		JavaRDD<Double> subtracRDD = subtractRDDElements(randomPartitionRDD, this.rdd().toJavaRDD());
		
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
	private JavaRDD<Double> subtractRDDElements(JavaRDD<Double> rdd1, JavaRDD<Double>  rdd2) {
		
		ArrayList<Double> collectRDD1 = new ArrayList<>(rdd1.collect());
		ArrayList<Double> collectRDD2 = new ArrayList<>(rdd2.collect());
		
		for (Double obj : collectRDD1) {
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
	 * @param javaRDD
	 * @return
	 */
	private JavaRDD<Double> getRandomPartition(JavaRDD<Double> javaRDD) {
		
		int randomPartition = (Math.abs(rand.nextInt()) % rdd().getNumPartitions());
		
		ArrayList<Double> collectRDD = new ArrayList<> (javaRDD.glom().collect().get(randomPartition));
		
		return this.context().parallelize(JavaConversions.asScalaBuffer(collectRDD), 1, this.classTag()).toJavaRDD();
	}

}
