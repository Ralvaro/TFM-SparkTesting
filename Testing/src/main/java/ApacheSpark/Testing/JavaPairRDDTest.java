package ApacheSpark.Testing;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Clase que representa un objeto JavaPairRDDTest que contiene los metodos para testear las funciones que dispone un JavaPairRDD.
 * @author Alvaro R. Perez Entrenas
 */
public class JavaPairRDDTest extends JavaPairRDD {

	/**
	 * Serial Version ID por defecto.
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructor dado un <b>JavaPairRDD</b>.
	 * @param rdd
	 */
	public JavaPairRDDTest(JavaPairRDD rdd) {
		super(rdd.toRDD(rdd), rdd.kClassTag(), rdd.vClassTag());
	}

	/**
	 * Constructor dado un <b>RDD</b> y los <b>ClassTag</b> de su clave y valor.
	 * @param rdd (RDD).
	 * @param kClassTag (ClassTag).
	 * @param vClassTag (ClassTag).
	 */
	public JavaPairRDDTest(RDD rdd, ClassTag kClassTag, ClassTag vClassTag) {
		super(rdd, kClassTag, vClassTag);
	}
	
	/**
	 * Metodo de test para una funcion <b>reduceByKey()</b>.
	 * @param function (Function2<Object, Object, Object>) - Funcion reduce que se desea testear.
	 */
	public void reduceByKeyTest(Function2<Object, Object, Object> function) {
		
		// Se cambia el numero de particiones
		JavaPairRDD<Object, Object> rdd1 = this.coalesce(1, false);
		JavaPairRDD<Object, Object> rdd2 = this.coalesce(2, false);
		JavaPairRDD<Object, Object> rdd3 = this.coalesce(3, false);
		
		JavaPairRDD<Object, Object> result = rdd1.reduceByKey(function).sortByKey();
		JavaPairRDD<Object, Object> result2 = rdd2.reduceByKey(function).sortByKey();
		JavaPairRDD<Object, Object> result3 = rdd3.reduceByKey(function).sortByKey();
		
		List<Tuple2<Object, Object>> collect1 = result.collect();
		List<Tuple2<Object, Object>> collect2 = result2.collect();
		List<Tuple2<Object, Object>> collect3 = result3.collect();
		
		Assert.assertEquals(collect1, collect2);
		Assert.assertEquals(collect1, collect3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaPairRDD<Object, Object> rdd4 = this.coalesce(3, true);
		JavaPairRDD<Object, Object> result4 = rdd4.reduceByKey(function).sortByKey();
		
		// Se comprueban que los resultados sean iguales
		List<Tuple2<Object, Object>> collect4 = result4.collect();
		
		Assert.assertEquals(collect3, collect4);
	}
}
