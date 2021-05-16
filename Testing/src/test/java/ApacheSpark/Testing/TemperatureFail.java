package ApacheSpark.Testing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Programa para calcular la media de temperaturas a lo largo de los años.
 * Este programa es erroneo. Servirá para verificar que el proyecto ApacheSparkTesting
 * es capaz de detectar errores de diseño.
 * @author Alvaro R. Perez Entrenas
 *
 */
public class TemperatureFail {
	
	public static void main(String[] args) {
		
		// Configurar el Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TemperatureFail");
		
		// Crear el Spark Context
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Se leen los datos de un fichero con los datos (podria configurarse otra fuente)
		JavaRDD<String> rdd = sc.textFile("temp.txt");
		
		// Se dividen los registros por ","
		JavaRDD<String[]> parse = rdd.map(l -> l.split(","));
		
		// JavaRDDTest usado para validar la operacion .mapToPair
		JavaRDDTest<String[]> rddTest = new JavaRDDTest<>(parse);
		
		// Se crea tuplas de cada registro con el año y la temperatura
		JavaPairRDD<String, Double> pair =  rddTest.mapToPair(p -> new Tuple2<>(p[0], Double.parseDouble(p[1].trim())));
		
		// JavaPairRDDTest usado para validar la operacion .reduceByKey
		JavaPairRDDTest<String, Double> pairTest = new JavaPairRDDTest<>(pair);
		
		// Se reducen los valores por la clave (el año)
		JavaPairRDD<String, Double> result = pairTest.reduceByKey((x, y) -> (x + y) / 2.0);
		
		// Se muestran los resultados
		System.out.println(result.collect());
		
		sc.close();
	}

}
