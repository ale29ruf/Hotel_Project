import static spark.Spark.*;

public class WebService {
    public static void main(String[] args) {
        // Specifica la porta personalizzata (ad esempio, 8080)
        port(8080);
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


