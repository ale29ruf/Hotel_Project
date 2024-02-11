import scala.collection.Map;

import static spark.Spark.*;

public class WebService {
    public static void main(String[] args) {
        // Specifica la porta personalizzata (ad esempio, 8080)
        port(8080);

        // Abilita CORS per consentire l'accesso da http://localhost:57479
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
            System.out.println("Chiamata pervenuta");
            Map<String, Object> result = NationalityScoreAnalysis.getNationalityScore();
            System.out.println("Chiamata in invio");
            return result;
        });

        before((request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Request-Method", "GET, POST, PUT, DELETE, OPTIONS");
            response.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        });
    }
}


