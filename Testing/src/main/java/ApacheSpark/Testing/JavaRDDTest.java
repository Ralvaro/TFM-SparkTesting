package ApacheSpark.Testing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;

import scala.reflect.ClassTag;

/**
 * Clase que representa un objeto JavaRDDTest que contiene los metodos para testear las funciones que dispone un RDD.
 * @author Alvaro R. Perez Entrenas
 *
 */
public class JavaRDDTest extends JavaRDD {
	
	/**
	 * ID Serial por defecto.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor dado un RDD y su ClassTag.
	 * @param rdd
	 * @param classTag
	 */
	public JavaRDDTest(RDD<?> rdd, ClassTag<?> classTag) {
		super(rdd, classTag);
	}
	
	/**
	 * Constructor dado un JavaRDD.
	 * @param rdd
	 */
	public JavaRDDTest(JavaRDD<?> rdd) {
		super(rdd.toRDD(rdd), rdd.classTag());
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo reduce con parametros Strings es correcta.
	 * 
	 * @param function (Function2<String, String, String>) - Funcion reduce que se desea testear.
	 * @return (boolean) - Resultado del test.
	 */
	public boolean reduceTestString(Function2<String, String, String> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<String> rdd1 = this.rdd().toJavaRDD().coalesce(1, false);
		JavaRDD<String> rdd2 = this.rdd().toJavaRDD().coalesce(2, false);
		JavaRDD<String> rdd3 = this.rdd().toJavaRDD().coalesce(3, false);
		
		String result1 = rdd1.reduce(function);
		String result2 = rdd2.reduce(function);
		String result3 = rdd3.reduce(function);
		
		// Se comprueban que sean iguales
		Assert.assertEquals(result1, result2);
		Assert.assertEquals(result1, result3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaRDD<String> rdd4 = this.rdd().toJavaRDD().coalesce(3, true);
		String result4 = rdd4.reduce(function);
		
		// Se comprueban que los resultados sean igual
		Assert.assertEquals(result3, result4);
		
		return true;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo reduce con parametros Integers es correcta. Se realizan
	 * las siguientes comprobaciones:
	 * - Propiedad Asociativa.
	 * - Propiedad Conmutativa.
	 * 
	 * @param function (Function2<Integer, Integer, Integer>) - Funcion reduce que se desea testear.
	 * @return (boolean) - Resultado del test.
	 */
	public boolean reduceTestInteger(Function2<Integer, Integer, Integer> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<Integer> rdd1 = this.rdd().toJavaRDD().coalesce(1, false);
		JavaRDD<Integer> rdd2 = this.rdd().toJavaRDD().coalesce(2, false);
		JavaRDD<Integer> rdd3 = this.rdd().toJavaRDD().coalesce(3, false);
		
		Integer result1 = rdd1.reduce(function);
		Integer result2 = rdd2.reduce(function);
		Integer result3 = rdd3.reduce(function);
		
		// Se comprueban que sean iguales
		Assert.assertEquals(result1, result2);
		Assert.assertEquals(result1, result3);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaRDD<Integer> rdd4 = this.rdd().toJavaRDD().coalesce(3, true);
		Integer result4 = rdd4.reduce(function);
		
		// Se comprueban que los resultados sean igual
		Assert.assertEquals(result3, result4);
		
		return true;
	}
	
	/**
	 * Metodo que comprueba si una funcion de tipo reduce con parametros Double es correcta.
	 * 
	 * @param function (Function2<Double, Double, Double>) - Funcion reduce que se desea testear.
	 * @return (boolean) - Resultado del test.
	 */
	public boolean reduceTestDouble(Function2<Double, Double, Double> function) {
		
		// Se cambia el numero de particiones
		JavaRDD<Double> rdd1 = this.rdd().toJavaRDD().coalesce(1, false);
		JavaRDD<Double> rdd2 = this.rdd().toJavaRDD().coalesce(2, false);
		JavaRDD<Double> rdd3 = this.rdd().toJavaRDD().coalesce(3, false);
		
		Double result1 = rdd1.reduce(function);
		Double result2 = rdd2.reduce(function);
		Double result3 = rdd3.reduce(function);
		
		// Se comprueban que sean iguales
		Assert.assertEquals(result1.compareTo(result2), 0);
		Assert.assertEquals(result1.compareTo(result3), 0);
		
		// Se cambia el orden las particiones (mismas particiones que rdd3 pero con los datos desordenados)
		JavaRDD<Double> rdd4 = this.rdd().toJavaRDD().coalesce(3, true);
		
		Double result4 = rdd4.reduce(function);
		
		// Se comprueban que los resultados sean igual
		Assert.assertEquals(result3.compareTo(result4), 0);
		
		return true;
	}

}
