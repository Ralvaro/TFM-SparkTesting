package ApacheSpark.Testing;

import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Assert;


/**
 * Clase que representa un objeto JavaPairRDDTest que contiene los metodos para testing de los metodos que dispone la clase JavaPairRDD.
 * @see JavaPairRDD
 * @author Alvaro R. Perez Entrenas
 */
public class JavaPairRDDTest<K, V> extends JavaPairRDD<K, V> {
	
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
	 * Constructor dado un <b>JavaPairRDD</b>.
	 * @param rdd
	 */
	public JavaPairRDDTest(JavaPairRDD<K, V> rdd) {
		super(rdd.rdd(), rdd.kClassTag(), rdd.vClassTag());
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
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param f (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> f) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		JavaPairRDD<K, V> result = pairRDD.reduceByKey(f).sortByKey().cache();
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, V> resultToCompare = pairRDD.coalesce(1, false).reduceByKey(f).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		resultToCompare = pairRDD.coalesce(maxNumPartitions, false).reduceByKey(f).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = pairRDD.reduceByKey(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = pairRDD.coalesce(randomNumber, false).reduceByKey(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = pairRDD.coalesce(this.rdd().getNumPartitions(), true).reduceByKey(f).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param f (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @param numPartitions (int) - Numero de particiones deseadas.
	 * @return JavaPairRDD<K, V> - Resultado de la operacion.
	 */
	public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> f, int numPartitions) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		JavaPairRDD<K, V> result = pairRDD.reduceByKey(f, numPartitions).sortByKey().cache();
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, V> resultToCompare = pairRDD.coalesce(1, false).reduceByKey(f, numPartitions).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		resultToCompare = pairRDD.coalesce(maxNumPartitions, false).reduceByKey(f, numPartitions).sortByKey();
		Assert.assertEquals(result.collect(), resultToCompare.collect());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = pairRDD.reduceByKey(f, numPartitions).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = pairRDD.coalesce(randomNumber, false).reduceByKey(f, numPartitions).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = pairRDD.coalesce(this.rdd().getNumPartitions(), true).reduceByKey(f, numPartitions).sortByKey();
			Assert.assertEquals(result.collect(), resultToCompare.collect());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param zeroValue (U) - 
	 * @param function (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, U> - Resultado de la operacion.
	 */
	public <U> JavaPairRDD<K, U> aggregateByKey(U zeroValue, Function2<U, V, U> seqFunc, Function2<U, U, U> combFunc) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		JavaPairRDD<K, U> result = pairRDD.aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey().cache();
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, U> resultToCompare = pairRDD.coalesce(1, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		
		resultToCompare = pairRDD.coalesce(maxNumPartitions, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = pairRDD.aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = pairRDD.coalesce(randomNumber, false).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = pairRDD.coalesce(this.rdd().getNumPartitions(), true).aggregateByKey(zeroValue, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		}
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduceByKey()</b>.
	 * 	<ul>
	 * 		<li>Comprueba que sea idempotente.</li>
	 * 		<li>Comprueba que sea asociativa.</li>
	 * 		<li>Comprueba que sea conmutativa.</li>
	 * 	</ul>
	 * @param zeroValue (U) - 
	 * @param function (Function2<V, V, V>) - Funcion reduceByKey que se desea testear.
	 * @return JavaPairRDD<K, U> - Resultado de la operacion.
	 */
	public <U> JavaPairRDD<K, U> aggregateByKey(U zeroValue, int numPartitions, Function2<U, V, U> seqFunc, Function2<U, U, U> combFunc) {
		
		JavaPairRDD<K, V> pairRDD = this.cache();
		JavaPairRDD<K, U> result = pairRDD.aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey().cache();
		
		// Se comprueba que sea asociativa cambiando el numero de particiones
		JavaPairRDD<K, U> resultToCompare = pairRDD.coalesce(1, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		
		resultToCompare = pairRDD.coalesce(maxNumPartitions, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
		Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		
		for (int i = 0; i < numRepetitions; i++) {
			
			// Se comprueba que sea idempotente ejecutando multiples veces con el mismo resultado
			resultToCompare = pairRDD.aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
			
			// Se comprueba que sea asociativa cambiando el numero de particiones
			int randomNumber = (Math.abs(rand.nextInt()) % this.rdd().getNumPartitions()) + 1;
			
			resultToCompare = pairRDD.coalesce(randomNumber, false).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
			
			// Se comprueba la propiedad conmutativa cambiando el orden de las particiones
			resultToCompare = pairRDD.coalesce(this.rdd().getNumPartitions(), true).aggregateByKey(zeroValue, numPartitions, seqFunc, combFunc).sortByKey();
			Assert.assertEquals(result.collect().toString(), resultToCompare.collect().toString());
		}
		
		return result;
	}
	
}
