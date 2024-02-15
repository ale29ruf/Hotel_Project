import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static spark.Spark.*;

public class WebService {

    private static final SparkSession sparkContext = SparkSession.builder().appName("HotelApp")
            .config("spark.master", "local") // Esempio: esegui in modalità locale
            .getOrCreate();

    public static Dataset<Row> dataFrame = sparkContext.read()
            .option("header", "true")
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

        get("/nationalityScore", (request, response) -> {
            // Conversione della mappa in JSON
            return mapper.writeValueAsString(NationalityScoreAnalysis.getNationalityScore());
        });

        get("/Function1", (request, response) -> mapper.writeValueAsString(Function1.eseguiAnalisi()));

        get("/Function2", (request, response) -> mapper.writeValueAsString(Function2.eseguiAnalisi()));

        get("GetAllNationality", (request, response) -> mapper.writeValueAsString(GetNationalityReviewers.get()));
    }
}


