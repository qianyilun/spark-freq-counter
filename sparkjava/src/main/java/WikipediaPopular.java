import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class WikipediaPopular {

    public static void main(String[] args) {
    	int processors = Runtime.getRuntime().availableProcessors();

        String base = "./src/main/resources/";
        String folderName1 = "pagecounts-with-time-1/";
        String folderName2 = "pagecounts-with-time-2/";

        String folderPath1 = base + folderName1;
        String folderPath2 = base + folderName2;

        File folder1 = new File(folderPath1);
        File folder2 = new File(folderPath2);
        File[] listOfFiles1 = folder1.listFiles();
        File[] listOfFiles2 = folder2.listFiles();

        SparkConf sparkConf = new SparkConf().setAppName("WikipediaPopular.counts").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        long start = System.currentTimeMillis();

        for (File file : listOfFiles1) {
            if (file.isFile()) {
                countPopular(file.getAbsolutePath(), folderName1+file.getName(), sc);
            } else if (file.isDirectory()) {
                System.out.println("Directory " + file);
            }
        }

        for (File file : listOfFiles2) {
            if (file.isFile()) {
                countPopular(file.getAbsolutePath(), folderName2+file.getName(), sc);
            } else if (file.isDirectory()) {
                System.out.println("Directory " + file);
            }
        }

        long end = System.currentTimeMillis();

        System.out.println(String.format("Total time with %d cores CPU is %s ms", processors, end - start));
    }

    private static void countPopular(String filePath, String outputPath, JavaSparkContext sc) {



        JavaRDD<String> textFile = sc.textFile(filePath);


        textFile.map(line -> line.split(" "))
                .filter((Function<String[], Boolean>) value -> {
                    String lang = value[1];
                    String title = value[2];
                    return lang.equals("en") && !title.equals("Main_Page") && !title.startsWith("Special:");
                })
                .mapToPair(values -> {
                    String date = values[0];
                    int visits = Integer.parseInt(values[3]);
                    String title = values[2];
                    return new Tuple2(date, new Tuple2(title, visits));
                })
                .reduceByKey((Object a, Object b) -> {
                    Tuple2 t1 = (Tuple2) a;
                    Tuple2 t2 = (Tuple2) b;
                    if ((int)t1._2() >= (int)t2._2()) {
                        return t1;
                    }
                    return t2;
                })
                .sortByKey().map((Function) kv -> {
            Tuple2 t = (Tuple2) kv;
            Tuple2 t2 = (Tuple2) t._2();
            return String.format("%s\t(%s, '%s')", t._1, t2._2(), t2._1());
        })
                .saveAsTextFile("./src/main/out/" + outputPath);
    }
}
