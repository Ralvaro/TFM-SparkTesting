package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;

import scala.Tuple2;
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
	 * Metodo que comprueba si una funcion de tipo <b>reduce()</b>.
	 * @param function (Function2<Object, Object, Object>) - Funcion reduce que se desea testear.
	 */
	public T reduce(Function2<T, T, T> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<T> rdd1 = this.coalesce(1, false);
		JavaRDD<T> rdd2 = this.coalesce(2, false);
		JavaRDD<T> rdd3 = this.coalesce(3, false);
		
		T result = rdd1.reduce(function);
		T result2 = rdd2.reduce(function);
		T result3 = rdd3.reduce(function);
		
		Assert.assertEquals(result, result2);
		Assert.assertEquals(result, result3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaRDD<T> rdd4 = this.coalesce(3, true);
		T result4 = rdd4.reduce(function);
		
		// Se comprueban que los resultados sean igual
		Assert.assertEquals(result3, result4);
		
		return result;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>map()</b>.
	 *  - Simula que el numero de tareas mapper cambie.
	 *  - Simula el RDD este cacheado en memoria.
	 *  - Simula que un ejecutor falle.
	 * @param function (Function<Object, Object>) - Funcion map que se desea testear.
	 */
	public <R> JavaRDD<R> map(Function<T, R> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<T> rdd1 = this.coalesce(1, true);
		JavaRDD<T> rdd2 = this.coalesce(2, true);
		JavaRDD<T> rdd3 = this.coalesce(3, true);
		
		JavaRDD<R> result1 = rdd1.map(function).sortBy(f -> f, true, 1);
		JavaRDD<R> result2 = rdd2.map(function).sortBy(f -> f, true, 2);
		JavaRDD<R> result3 = rdd3.map(function).sortBy(f -> f, true, 3);
		
		List<R> collect1 = result1.collect();
		List<R> collect2 = result2.collect();
		List<R> collect3 = result3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Cacheo de RDD en memoria
		JavaRDD<T> rdd4 = rdd3.cache();
		JavaRDD<R> result4 = rdd4.map(function).sortBy(f -> f, true, 3);
		List<R> collect4 = result4.collect();
		
		Assert.assertEquals(collect3, collect4);
		
		// Que un mapper falle y se vuelva a ejecutar por separado
		JavaRDD<T> rdd5 = rdd1.sample(false, 0.25);
		JavaRDD<T> rdd6 = subtractRDDElements(rdd5, rdd1);
		
		JavaRDD<R> result5 = rdd5.map(function);
		JavaRDD<R> result6 = rdd6.map(function);
		
		JavaRDD<R> result7 = result5.union(result6).sortBy(f -> f, true, result6.getNumPartitions());
		
		List<R> collect5 = result7.collect();
		
		Assert.assertEquals(collect4, collect5);
		
		return result1;
	}
	
	public <K,V> JavaPairRDD<K, V> mapToPair(PairFunction<T, K, V> function) {
		
		// Numero de mappers cambie
		JavaRDD<T> rdd1 = this.coalesce(1, false);
		JavaRDD<T> rdd2 = this.coalesce(2, false);
		JavaRDD<T> rdd3 = this.coalesce(3, false);
		
		JavaPairRDD<K, V> pair1 = rdd1.mapToPair(function).sortByKey();
		JavaPairRDD<K, V> pair2 = rdd2.mapToPair(function).sortByKey();
		JavaPairRDD<K, V> pair3 = rdd3.mapToPair(function).sortByKey();
		
		List<Tuple2<K, V>> collect1 = pair1.collect();
		List<Tuple2<K, V>> collect2 = pair2.collect();
		List<Tuple2<K, V>> collect3 = pair3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Cacheo de RDD en memoria
		JavaRDD<T> rdd4 = rdd3.cache();
		JavaPairRDD<K, V> pair4 = rdd4.mapToPair(function).sortByKey();
		List<Tuple2<K, V>> collect4 = pair4.collect();
		Assert.assertEquals(collect3, collect4);
		
		// Que un mapper aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<T> rdd5 = rdd1.sample(false, 0.25);
		JavaRDD<T> rdd6 = subtractRDDElements(rdd5, rdd1);
		
		JavaPairRDD<K, V> pair5 = rdd5.mapToPair(function).sortByKey();
		JavaPairRDD<K, V> pair6 = rdd6.mapToPair(function).sortByKey();
		
		JavaPairRDD<K, V> pair7 = pair5.union(pair6).sortByKey();
		
		List<Tuple2<K, V>> collect5 = pair7.collect();
		
		Assert.assertEquals(collect4, collect5);
		
		return pair1;
	}
	
	
	
	/**
	 * Metodo que crea un nuevo RDD de los elementos 
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
		
		return this.context().parallelize(JavaConversions.asScalaBuffer(collectRDD2), rdd2.getNumPartitions(), this.classTag()).toJavaRDD();
	}

}
