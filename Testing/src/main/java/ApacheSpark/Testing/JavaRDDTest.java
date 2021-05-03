package ApacheSpark.Testing;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.Partition;
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
public class JavaRDDTest extends JavaRDD {
	
	/**
	 * ID Serial por defecto.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor dado un <b>RDD</b> y su <b>ClassTag</b>.
	 * @param rdd (RDD).
	 * @param classTag (ClassTagg).
	 */
	public JavaRDDTest(RDD<?> rdd, ClassTag<?> classTag) {
		super(rdd, classTag);
	}
	
	/**
	 * Constructor dado un JavaRDD.
	 * @param rdd (JavaRDD).
	 */
	public JavaRDDTest(JavaRDD<?> rdd) {
		super(rdd.rdd(), rdd.classTag());
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>reduce()</b>.
	 * @param function (Function2<Object, Object, Object>) - Funcion reduce que se desea testear.
	 */
	public void reduceTest(Function2<Object, Object, Object> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<Object> rdd1 = this.coalesce(1, false);
		JavaRDD<Object> rdd2 = this.coalesce(2, false);
		JavaRDD<Object> rdd3 = this.coalesce(3, false);
		
		Object result = rdd1.reduce(function);
		Object result2 = rdd2.reduce(function);
		Object result3 = rdd3.reduce(function);
		
		Assert.assertEquals(result, result2);
		Assert.assertEquals(result, result3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaRDD<Object> rdd4 = this.coalesce(3, true);
		Object result4 = rdd4.reduce(function);
		
		// Se comprueban que los resultados sean igual
		Assert.assertEquals(result3, result4);
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>map()</b>.
	 *  - Simula que el numero de tareas mapper cambie.
	 *  - Simula el RDD este cacheado en memoria.
	 *  - Simula que un ejecutor falle.
	 * @param function (Function<Object, Object>) - Funcion map que se desea testear.
	 */
	public void mapTest(Function<Object, Object> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<Object> rdd1 = this.coalesce(1, true);
		JavaRDD<Object> rdd2 = this.coalesce(2, true);
		JavaRDD<Object> rdd3 = this.coalesce(3, true);
		
		JavaRDD<Object> result1 = rdd1.map(function).sortBy(f -> f, true, 1);
		JavaRDD<Object> result2 = rdd2.map(function).sortBy(f -> f, true, 2);
		JavaRDD<Object> result3 = rdd3.map(function).sortBy(f -> f, true, 3);
		
		List<Object> collect1 = result1.collect();
		List<Object> collect2 = result2.collect();
		List<Object> collect3 = result3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Cacheo de RDD en memoria
		JavaRDD<Object> rdd4 = rdd3.cache();
		JavaRDD<Object> result4 = rdd4.map(function).sortBy(f -> f, true, 3);
		List<Object> collect4 = result4.collect();
		
		Assert.assertEquals(collect3, collect4);
		
		// Que un mapper falle y se vuelva a ejecutar por separado
		JavaRDD<Object> rdd5 = rdd1.sample(false, 0.25);
		JavaRDD<Object> rdd6 = subtractRDDElements(rdd5, rdd1);
		
		JavaRDD<Object> result5 = rdd5.map(function);
		JavaRDD<Object> result6 = rdd6.map(function);
		
		JavaRDD<Object> result7 = result5.union(result6).sortBy(f -> f, true, result6.getNumPartitions());
		
		List<Object> collect5 = result7.collect();
		
		Assert.assertEquals(collect4, collect5);
	}
	
	private JavaRDD<Object> subtractRDDElements(JavaRDD<Object> rdd1, JavaRDD<Object> rdd2) {
		
		ArrayList<Object> collectRDD1 = new ArrayList<Object>(rdd1.collect());
		ArrayList<Object> collectRDD2 = new ArrayList<Object>(rdd2.collect());
		
		for (Object obj : collectRDD1) {
			collectRDD2.remove(obj);
		}
		
		return this.context().parallelize(JavaConversions.asScalaBuffer(collectRDD2), rdd2.getNumPartitions(), this.classTag()).toJavaRDD();
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo <b>mapToPair()</b>.
	 *  - Simula que el numero de tareas mapper cambie.
	 *  - Simula el RDD este cacheado en memoria.
	 *  - Simula que un ejecutor falle.
	 * @param function (PairFunction<Object, Object, Object>) - Funcion mapToPair que se desea testear.
	 */
	public void mapToPairTest(PairFunction<Object, Object, Object> function) {
		
		// Numero de mappers cambie
		JavaRDD<Object> rdd1 = this.coalesce(1, false);
		JavaRDD<Object> rdd2 = this.coalesce(2, false);
		JavaRDD<Object> rdd3 = this.coalesce(3, false);
		
		JavaPairRDD<Object, Object> pair1 = rdd1.mapToPair(function).sortByKey();
		JavaPairRDD<Object, Object> pair2 = rdd2.mapToPair(function).sortByKey();
		JavaPairRDD<Object, Object> pair3 = rdd3.mapToPair(function).sortByKey();
		
		List<Tuple2<Object, Object>> collect1 = pair1.collect();
		List<Tuple2<Object, Object>> collect2 = pair2.collect();
		List<Tuple2<Object, Object>> collect3 = pair3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Cacheo de RDD en memoria
		JavaRDD<Object> rdd4 = rdd3.cache();
		JavaPairRDD<Object, Object> pair4 = rdd4.mapToPair(function).sortByKey();
		List<Tuple2<Object, Object>> collect4 = pair4.collect();
		Assert.assertEquals(collect3, collect4);
		
		// Que un mapper aleatorio falle y se vuelva a ejecutar por separado
		JavaRDD<Object> rdd5 = rdd1.sample(false, 0.25);
		JavaRDD<Object> rdd6 = subtractRDDElements(rdd5, rdd1);
		
		JavaPairRDD<Object, Object> pair5 = rdd5.mapToPair(function).sortByKey();
		JavaPairRDD<Object, Object> pair6 = rdd6.mapToPair(function).sortByKey();
		
		JavaPairRDD<Object, Object> pair7 = pair5.union(pair6).sortByKey();
		
		List<Tuple2<Object, Object>> collect5 = pair7.collect();
		
		Assert.assertEquals(collect4, collect5);
	}

}
