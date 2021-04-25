package ApacheSpark.Testing;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;

import scala.reflect.ClassTag;

public class JavaPairRDDTest extends JavaPairRDD {

	/**
	 * Serial Version ID por defecto.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor dado un RDD y los ClassTag de su clave y valor.
	 * @param rdd
	 * @param kClassTag
	 * @param vClassTag
	 */
	public JavaPairRDDTest(RDD rdd, ClassTag kClassTag, ClassTag vClassTag) {
		super(rdd, kClassTag, vClassTag);
	}
	

}
