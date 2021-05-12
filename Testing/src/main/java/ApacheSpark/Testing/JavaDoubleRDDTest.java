package ApacheSpark.Testing;

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
	 *  - Comprueba que sea idempotente.
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
	 *  - Comprueba que sea idempotente.
	 * @param function (Function<Double, Boolean>) - Funcion filter que se desea testear.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD filter(Function<Double, Boolean> function) {
		
		JavaDoubleRDD result = this.filter(function).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.filter(function);
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMap()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (FlatMapFunction<T, U>) - Funcion flatMap que se desea testear.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> flatMap(FlatMapFunction<Double, U> f) {
		
		JavaRDD<U> result = this.flatMap(f).sortBy(e -> e, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.flatMap(f).sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return null;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMapToPair()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param function (PairFlatMapFunction<T, K2, V2>) - Funcion flatMapToPair que se desea testear.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> flatMapToPair(PairFlatMapFunction<Double, K2, V2> f) {
		
		JavaPairRDD<K2, V2> result = this.flatMapToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.flatMapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>flatMapToPair()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param function (PairFlatMapFunction<T, K2, V2>) - Funcion flatMapToPair que se desea testear.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<Double> f) {
		
		JavaDoubleRDD result = this.flatMapToDouble(f).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.flatMapToDouble(f);
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>map()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function<T, R>) - Funcion map que se desea testear.
	 * @return JavaRDD<R> - Resultado de la operacion.
	 */
	public <R> JavaRDD<R> map(Function<Double, R> f) {
		
		JavaRDD<R> result = this.rdd().toJavaRDD().map(f);
		result = result.sortBy(e -> e, true, result.getNumPartitions());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = this.rdd().toJavaRDD().map(f);
			resultToCompare = resultToCompare.sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapToPair()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (PairFunction<T, K, V>) - Funcion mapToPair que se desea testear.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public <K2,V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<Double, K2, V2> f) {
		
		
		JavaPairRDD<K2, V2> result = this.mapToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.mapToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitions()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param function (Function<Object, Object>) - Funcion mapPartitions que se desea testear.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<Double>, U> function) {
		
		JavaRDD<U> result = this.rdd().toJavaRDD().mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.rdd().toJavaRDD().mapPartitions(function).sortBy(f -> f, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitions()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function<Object, Object>) - Funcion mapPartitions que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaRDD<U> - Resultado de la operacion.
	 */
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<Double>, U> f, boolean preservesPartitioning) {
		
		JavaRDD<U> result = this.rdd().toJavaRDD().mapPartitions(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<U> resultToCompare = this.rdd().toJavaRDD().mapPartitions(f, preservesPartitioning);
			resultToCompare = resultToCompare.sortBy(e -> e, true, this.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToDouble()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function<Object, Object>) - Funcion mapPartitionsToDouble que se desea testear.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Double>> f) {
				
		JavaDoubleRDD result = this.rdd().toJavaRDD().mapPartitionsToDouble(f).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.rdd().toJavaRDD().mapPartitionsToDouble(f).cache();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToDouble()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function<Object, Object>) - Funcion mapPartitionsToDouble que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaDoubleRDD - Resultado de la operacion.
	 */
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Double>> f, boolean preservesPartitioning) {
		
		JavaDoubleRDD result = this.mapPartitionsToDouble(f, preservesPartitioning).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaDoubleRDD resultToCompare = this.mapPartitionsToDouble(f, preservesPartitioning).cache();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToPair()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (PairFlatMapFunction<Iterator<T>, K2, V2>) - Funcion mapPartitionsToPair que se desea testear.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Double>, K2, V2> f) {
		
		JavaPairRDD<K2, V2> result = this.mapPartitionsToPair(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.mapPartitionsToPair(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsToPair()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (PairFlatMapFunction<Iterator<T>, K2, V2>) - Funcion mapPartitionsToPair que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaPairRDD<K2, V2> - Resultado de la operacion.
	 */
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Double>, K2, V2> f, boolean preservesPartitioning) {
		
		JavaPairRDD<K2, V2> result = this.mapPartitionsToPair(f, preservesPartitioning).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K2, V2> resultToCompare = this.mapPartitionsToPair(f, preservesPartitioning).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapPartitionsWithIndex()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function2<Integer, Iterator<T>, Iterator<R>>) - Funcion mapPartitionsWithIndex que se desea testear.
	 * @param preservesPartitioning (Boolean) - Indica si se conserva el particionado.
	 * @return JavaRDD<R> - Resultado de la operacion.
	 */
	public <R> JavaRDD<R> mapPartitionsWithIndex(Function2<Integer, Iterator<Double>, Iterator<R>> f, boolean preservesPartitioning) {
		
		JavaRDD<R> result = this.mapPartitionsWithIndex(f, preservesPartitioning);
		result = result.sortBy(e -> e, true, result.getNumPartitions()).cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaRDD<R> resultToCompare = this.mapPartitionsWithIndex(f, preservesPartitioning);
			resultToCompare.sortBy(e -> e, true, result.getNumPartitions());
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}

}
