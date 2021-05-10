package ApacheSpark.Testing;

import java.util.Random;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Assert;


/**
 * Clase que representa un objeto JavaPairRDDTest que contiene los metodos para testear las funciones que dispone un JavaPairRDD.
 * @author Alvaro R. Perez Entrenas
 */
public class JavaPairRDDTest<K, V> extends JavaPairRDD<K, V> {
	
	/**
	 * Serial Version ID por defecto.
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constante para determinar el numero de repeticiones (10 por defecto).
	 */
	private int numRepetitions = 10;
	
	/**
	 * Objeto para obtener numeros aleatorios (12345 semilla por defecto).
	 */
	private Random rand = new Random(12345);
	
	/**
	 * Constante para determinar el numero maximo de particiones de un RDD.
	 */
	private static final int MAX_NUM_PARTITIONS = 99999;
	
	/**
	 * Constructor dado un <b>JavaPairRDD</b>.
	 * @param rdd
	 */
	public JavaPairRDDTest(JavaPairRDD<K, V> rdd) {
		super(rdd.rdd(), rdd.kClassTag(), rdd.vClassTag());
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
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> f) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, V> result = pairRDD.reduceByKey(f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, V> resultToCompare = pairRDD.reduceByKey(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, V> resultToCompare = pairRDD.coalesce(1, false).reduceByKey(f).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).reduceByKey(f).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).reduceByKey(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
//		// Se cambia el numero de particiones
//		JavaPairRDD<K, V> rdd1 = this.coalesce(1, false);
//		JavaPairRDD<K, V> rdd2 = this.coalesce(2, false);
//		JavaPairRDD<K, V> rdd3 = this.coalesce(3, false);
//		
//		JavaPairRDD<K, V> result = rdd1.reduceByKey(function).sortByKey();
//		JavaPairRDD<K, V> result2 = rdd2.reduceByKey(function).sortByKey();
//		JavaPairRDD<K, V> result3 = rdd3.reduceByKey(function).sortByKey();
//		
//		List<Tuple2<K, V>> collect1 = result.collect();
//		List<Tuple2<K, V>> collect2 = result2.collect();
//		List<Tuple2<K, V>> collect3 = result3.collect();
//		
//		Assert.assertEquals(collect1, collect2);
//		Assert.assertEquals(collect1, collect3);
//		
//		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
//		JavaPairRDD<K, V> rdd4 = this.coalesce(3, true);
//		JavaPairRDD<K, V> result4 = rdd4.reduceByKey(function).sortByKey();
//		
//		// Se comprueban que los resultados sean iguales
//		List<Tuple2<K, V>> collect4 = result4.collect();
//		
//		Assert.assertEquals(collect3, collect4);
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param f (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @param numPartitions (int) - Numero de particiones deseadas.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> f, int numPartitions) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, V> result = pairRDD.reduceByKey(f, numPartitions).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, V> resultToCompare = pairRDD.reduceByKey(f, numPartitions).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, V> resultToCompare = pairRDD.coalesce(1, false).reduceByKey(f, numPartitions).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).reduceByKey(f, numPartitions).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).reduceByKey(f, numPartitions).sortByKey();
			Assert.assertEquals(result, resultToCompare);
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param partitioner (Partitioner) - Particionador.
	 * @param f (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public JavaPairRDD<K, V> reduceByKey(Partitioner partitioner, Function2<V, V, V> f) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, V> result = pairRDD.reduceByKey(partitioner, f).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, V> resultToCompare = pairRDD.reduceByKey(partitioner, f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, V> resultToCompare = pairRDD.coalesce(1, false).reduceByKey(partitioner, f).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).reduceByKey(partitioner, f).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).reduceByKey(partitioner, f).sortByKey();
			Assert.assertEquals(result, resultToCompare);
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param zeroValue (U) - 
	 * @param function (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, U> - Resultado de la operacion.
	 */
	public <U> JavaPairRDD<K, U> aggregateByKey(U zeroValue, Function2<U, V, U> seqFunc, Function2<U, U, U> combFunc) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, U> result = pairRDD.aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, U> resultToCompare = pairRDD.aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, U> resultToCompare = pairRDD.coalesce(1, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result, resultToCompare);
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param zeroValue (U) - 
	 * @param function (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, U> - Resultado de la operacion.
	 */
	public <U> JavaPairRDD<K, U> aggregateByKey(U zeroValue, int numPartitions, Function2<U, V, U> seqFunc, Function2<U, U, U> combFunc) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, U> result = pairRDD.aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, U> resultToCompare = pairRDD.aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, U> resultToCompare = pairRDD.coalesce(1, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result, resultToCompare);
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 *  - Comprueba que sea idempotente.
	 * @param zeroValue (U) - 
	 * @param function (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, U> - Resultado de la operacion.
	 */
	public <U> JavaPairRDD<K, U> aggregateByKey(U zeroValue, Partitioner partitioner, Function2<U, V, U> seqFunc, Function2<U, U, U> combFunc) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		
		// Se comprueba que sea idempotente ejecutando multiples veces
		JavaPairRDD<K, U> result = pairRDD.aggregateByKey(zeroValue, partitioner, seqFunc, combFunc).sortByKey().cache();
		
		for (int i = 0; i < numRepetitions; i++) {
			
			JavaPairRDD<K, U> resultToCompare = pairRDD.aggregateByKey(zeroValue, partitioner, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, U> resultToCompare = pairRDD.coalesce(1, false).aggregateByKey(zeroValue, partitioner, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		resultToCompare = pairRDD.coalesce(MAX_NUM_PARTITIONS, false).aggregateByKey(zeroValue, partitioner, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result, resultToCompare);
		
		for (int i = 0; i < numRepetitions; i++) {
			
			resultToCompare = pairRDD.coalesce(rand.nextInt(this.getNumPartitions())+1, false).aggregateByKey(zeroValue, partitioner, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result, resultToCompare);
		}
		
		return result;
	}
	
}
