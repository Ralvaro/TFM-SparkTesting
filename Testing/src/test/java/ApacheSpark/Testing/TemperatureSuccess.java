package ApacheSpark.Testing;

import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Programa para calcular la media de temperaturas a lo largo de los años.
 * @author Alvaro R. Perez Entrenas
 *
 */
public class TemperatureSuccess {

	public static void main(String[] args) {
		
		// Configurar el Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TemperatureSuccess");
		
		// Crear el Spark Context
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Se leen los datos de un fichero con los datos (podria configurarse otra fuente)
		JavaRDD<String> rdd = sc.textFile("temp.txt");
		
		// Se dividen los registros por ","
		JavaRDD<String[]> parse = rdd.map(l -> l.split(","));
		
		// JavaRDDTest usado para validar la operacion .mapToPair
		JavaRDDTest<String[]> rddTest2 = new JavaRDDTest<>(parse);
		
		// Se crea tuplas de cada registro con el año y la temperatura
		JavaPairRDD<String, Integer> pair =  rddTest2.mapToPair(p -> new Tuple2<>(p[0], Integer.parseInt(p[1].trim())));
		
		// JavaPairRDDTest usado para validar la operacion .reduceByKey
		JavaPairRDDTest<String, Integer> pairTest = new JavaPairRDDTest<>(pair);
		
		// Se reducen los valores sumando los valores por año
		JavaPairRDD<String, Integer> sum = pairTest.reduceByKey((x, y) -> (x + y));
		
		// Se cuenta el numero de apariciones de cada clave
		Map<String, Long> count = pairTest.countByKey();
		
		ArrayList<Tuple2<String, Double>> resultList = new ArrayList<>();
		
		// De forma local se divide la suma de todos los valores de cada clave por el numero de veces que aparece 
		for(Tuple2<String, Integer> i : sum.collect()) {
			Long value = count.get(i._1);
			resultList.add(new Tuple2<String, Double>(i._1,  ( (double) i._2 / (double) value)));
		}
		
		for(Tuple2<String, Double> i : resultList) {
			System.out.println(i._1 + ": " + i._2);
		}
		
		sc.close();
		
	}
}
