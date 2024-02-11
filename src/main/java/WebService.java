import static spark.Spark.*;

public class WebService {
    public static void main(String[] args) {
        // Specifica la porta personalizzata (ad esempio, 8080)
        port(8080);

        // Definizione del percorso per le chiamate HTTP GET
        get("/hello", (request, response) -> {
            return "Hello World!";
        });

        // Definizione del percorso per le chiamate HTTP POST
        post("/hello", (request, response) -> {
            String name = request.queryParams("name");
            return "Hello, " + name + "!";
        });

        get("/nationalityScore", (request, response) -> {
            return NationalityScoreAnalysis.getNationalityScore();
        });
    }
}


