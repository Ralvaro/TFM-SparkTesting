package ApacheSpark.Testing;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Assert;

import scala.Tuple2;

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
	 * Constructor dado un <b>JavaPairRDD</b>.
	 * @param rdd
	 */
	public JavaPairRDDTest(JavaPairRDD<K, V> rdd) {
		super(rdd.rdd(), rdd.kClassTag(), rdd.vClassTag());
	}
	
	/**
	 * Metodo de test para una funcion <b>reduceByKey()</b>.
	 * @param function (Function2<Object, Object, Object>) - Funcion reduce que se desea testear.
	 */
	public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> function) {
		
		// Se cambia el numero de particiones
		JavaPairRDD<K, V> rdd1 = this.coalesce(1, false);
		JavaPairRDD<K, V> rdd2 = this.coalesce(2, false);
		JavaPairRDD<K, V> rdd3 = this.coalesce(3, false);
		
		JavaPairRDD<K, V> result = rdd1.reduceByKey(function).sortByKey();
		JavaPairRDD<K, V> result2 = rdd2.reduceByKey(function).sortByKey();
		JavaPairRDD<K, V> result3 = rdd3.reduceByKey(function).sortByKey();
		
		List<Tuple2<K, V>> collect1 = result.collect();
		List<Tuple2<K, V>> collect2 = result2.collect();
		List<Tuple2<K, V>> collect3 = result3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaPairRDD<K, V> rdd4 = this.coalesce(3, true);
		JavaPairRDD<K, V> result4 = rdd4.reduceByKey(function).sortByKey();
		
		// Se comprueban que los resultados sean iguales
		List<Tuple2<K, V>> collect4 = result4.collect();
		
		Assert.assertEquals(collect3, collect4);
		
		return result;
	}
}
