package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
	public T reduceTest(Function2<T, T, T> function) {
		
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
	public JavaRDD<T> mapTest(Function<T, T> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<T> rdd1 = this.coalesce(1, true);
		JavaRDD<T> rdd2 = this.coalesce(2, true);
		JavaRDD<T> rdd3 = this.coalesce(3, true);
		
		JavaRDD<T> result1 = rdd1.map(function).sortBy(f -> f, true, 1);
		JavaRDD<T> result2 = rdd2.map(function).sortBy(f -> f, true, 2);
		JavaRDD<T> result3 = rdd3.map(function).sortBy(f -> f, true, 3);
		
		List<T> collect1 = result1.collect();
		List<T> collect2 = result2.collect();
		List<T> collect3 = result3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Cacheo de RDD en memoria
		JavaRDD<T> rdd4 = rdd3.cache();
		JavaRDD<T> result4 = rdd4.map(function).sortBy(f -> f, true, 3);
		List<T> collect4 = result4.collect();
		
		Assert.assertEquals(collect3, collect4);
		
		// Que un mapper falle y se vuelva a ejecutar por separado
		JavaRDD<T> rdd5 = rdd1.sample(false, 0.25);
		JavaRDD<T> rdd6 = subtractRDDElements(rdd5, rdd1);
		
		JavaRDD<T> result5 = rdd5.map(function);
		JavaRDD<T> result6 = rdd6.map(function);
		
		JavaRDD<T> result7 = result5.union(result6).sortBy(f -> f, true, result6.getNumPartitions());
		
		List<T> collect5 = result7.collect();
		
		Assert.assertEquals(collect4, collect5);
		
		return result1;
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
