import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {

    //the argument will be the file path
    public static void main(String[] args)  {
        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        String file_path = args[0];
        //String file_path = "/Users/dinesh/Desktop/AgeGroupDetails.csv";
        String option = file_path.substring(file_path.length()-3,file_path.length());
        String mid_path = file_path.substring(0,file_path.length()-3);
        String out_path = mid_path + "parquet";
        System.out.println(out_path);
        System.out.println(option);
        switch(option){
            case "csv":
                Dataset<Row> df = spark.read().format("csv")
                        .option("sep", ",")
                        .option("inferSchema", "true")
                        .option("header", "true")
                        .load(file_path);
                df.write().parquet(out_path);
                System.out.println("Task Successful");
                break;
            case "tbl":
                Dataset<Row> df2 = spark.read()
                        .option("sep", "|")
                        .option("inferSchema", "true")
                        .option("header", "false")
                        .csv(file_path);
                df2.write().parquet(out_path);
                System.out.println("Task Successful");
                break;
            default:
                System.out.println("File Type Undefined");
        }

        spark.stop();
    }
}
