package ApacheSpark.Testing;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.Function2;
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
	 * Metodo de test para una funcion <b>reduce()</b>.
	 * @param function (Function2<Double, Double, Double>) - Funcion reduce que se desea testear.
	 */
	public void reduceTest(Function2<Double, Double, Double> function) {
		
		// Se cambia el numero de particiones
		JavaDoubleRDD rdd1 = this.coalesce(1, false);
		JavaDoubleRDD rdd2 = this.coalesce(2, false);
		JavaDoubleRDD rdd3 = this.coalesce(3, false);
		
		Double result1 = rdd1.reduce(function);
		Double result2 = rdd2.reduce(function);
		Double result3 = rdd3.reduce(function);
		
		Assert.assertEquals(result1, result2);
		Assert.assertEquals(result1, result3);
		
		// Se cambia el orden de las particiones
		JavaDoubleRDD rdd4 = this.coalesce(3, true);
		Double result4 = rdd4.reduce(function);
		
		Assert.assertEquals(result3, result4);
	}

}
