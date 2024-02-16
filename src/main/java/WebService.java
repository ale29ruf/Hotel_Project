import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static spark.Spark.*;

public class WebService {

    private static final SparkSession spark = SparkSession.builder().appName("HotelApp")
            .config("spark.master", "local") // esegui in modalità locale
            .getOrCreate();
    /*
    public static Dataset<Row> dataFrame = spark.read()
            .option("header", "true")
            .csv("database/Hotel_Reviews.csv");
    */
    public static Dataset<Row> dataFrame = spark.read()
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


        get("/Function1/:nationality", (request, response) -> mapper.writeValueAsString(Function1.eseguiAnalisi(request.params(":nationality"))));

        get("/Function2", (request, response) -> mapper.writeValueAsString(Function2.eseguiAnalisi()));

        get("GetAllNationality", (request, response) -> mapper.writeValueAsString(GetNationalityReviewers.get()));

        get("GetAllTags", (request, response) -> mapper.writeValueAsString(GetTags.get()));

        get("/Function3", (request, response) -> mapper.writeValueAsString(Function3.eseguiAnalisi()));

        //-------------------------------------------------------------------------------------------

        get("/Function4", (request, response) -> mapper.writeValueAsString(Function4.eseguiAnalisi()));

        get("/nationality", (request, response) -> mapper.writeValueAsString(Function4.getAllNationality()));

        get("/Function5", (request, response) -> mapper.writeValueAsString(Function5.eseguiAnalisi()));

    }
}


