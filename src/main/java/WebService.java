import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Map;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;


import static spark.Spark.*;

public class WebService {

    private static final SparkSession spark = SparkSession.builder().appName("HotelApp")
            .config("spark.master", "local") // esegui in modalità locale
            .getOrCreate();

    public static Dataset<Row> dataFrame = spark.read()
            .option("header", "true")
            .csv("database/Hotel_Reviews.csv");


    public static Dataset<Row> dataFrame1 = spark.read()
            .option("header", "true") // Se la prima riga è l'intestazione
            .option("inferSchema", "true") // Inferisci automaticamente il tipo di dati delle colonne
            .csv("database/Hotel_Reviews.csv");

    public static void main(String[] args) {
        // Specifica la porta personalizzata (ad esempio, 8080)
        port(8080);

        // Creazione dell'ObjectMapper
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new DefaultScalaModule());

        // Abilita CORS per tutte le origini e tutte le richieste
        options("/*", (request, response) -> {
            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }

            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if (accessControlRequestMethod != null) {
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }

            return "OK";
        });


        // Abilita CORS per tutte le origini
        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));

        // Definizione del percorso per le chiamate HTTP POST
        post("/hello", (request, response) -> {
            String name = request.queryParams("name");
            return "Hello, " + name + "!";
        });

        Map<String, List<Tuple2<String, Object>>> func1 = Function1.eseguiAnalisi();
        get("/Function1/:nationality", (request, response) -> mapper.writeValueAsString(func1.get(" "+request.params(":nationality")+" ")));

        Map<String, Map<String, Object>> func2 = Function2.eseguiAnalisi();
        get("/Function2", (request, response) -> mapper.writeValueAsString(func2));

        List<String> nation = GetNationalityReviewers.get();
        get("GetAllNationality", (request, response) -> mapper.writeValueAsString(nation));

        List<String> tags=GetTags.get();
        get("GetAllTags", (request, response) -> mapper.writeValueAsString(tags));

        Tuple2<String, Seq<String>>[] fun3 = Function3.eseguiAnalisi();
        get("/Function3", (request, response) -> mapper.writeValueAsString(fun3));

        //-------------------------------------------------------------------------------------------
        Map<String, Object> fun4 = Function4.eseguiAnalisi();
        get("/Function4", (request, response) -> mapper.writeValueAsString(fun4));

        Map<String, Tuple3<Object, Object, Object>> nationality = Function5.eseguiAnalisi();
        get("/nationality", (request, response) -> mapper.writeValueAsString(nationality));

        Map<String, Tuple3<Object, Object, Object>> fun5 = Function5.eseguiAnalisi();
        get("/Function5", (request, response) -> mapper.writeValueAsString(fun5));

        List<List<String>> fun6=Function6.eseguiAnalisi();
        get("/Function6", (request, response) -> mapper.writeValueAsString(fun6));

    }
}


