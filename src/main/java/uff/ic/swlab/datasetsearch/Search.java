package uff.ic.swlab.datasetsearch;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class Search extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final String DOMAIN = "localhost";
    private static final String SPARQL_PORT = ":8080";
    private static final String SPARQL_URL_TEMPLATE = "http://" + DOMAIN + SPARQL_PORT + "/fuseki/%1s";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String path = request.getPathInfo();
        String id = request.getParameter("id");
        String accept = request.getHeader("Accept");
        Lang lang = detectRequestedLang(accept);

        if (path == null && id != null)
            if (lang == null) {
                String url = "http://linkeddata.uriburner.com/about/html/" + ("" + request.getRequestURL()).replaceFirst("http://", "http/") + "/" + id + "&@Lookup@=&refresh=clean";
                response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                response.setHeader("Location", url);
            } else
                try (OutputStream httpReponse = response.getOutputStream()) {
                    Model model = getDescription(id);
                    if (model.size() > 0) {
                        response.setContentType(lang.getContentType().getContentType());
                        RDFDataMgr.write(httpReponse, model, lang);
                        httpReponse.flush();
                    } else
                        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                }
        else if (path != null && path.split("/").length == 2 && request.getQueryString() == null) {
            String url = ("" + request.getRequestURL()).replaceFirst(path, "") + "?id=" + path.replaceAll("/", "");
            response.setStatus(HttpServletResponse.SC_SEE_OTHER);
            response.setHeader("Location", url);
        } else
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    private Lang detectRequestedLang(String accept) {
        Lang[] langs = {Lang.RDFXML, Lang.TURTLE, Lang.TTL, Lang.TRIG, Lang.TRIX, Lang.JSONLD, Lang.RDFJSON,
            Lang.RDFTHRIFT, Lang.NQUADS, Lang.NQ, Lang.NTRIPLES, Lang.N3, Lang.NT};
        for (Lang lang : langs)
            if (accept.toLowerCase().contains(lang.getHeaderString().toLowerCase()))
                return lang;
        return null;
    }

    private static Model getDescription(String uri) throws UnsupportedEncodingException {

        Model model = ModelFactory.createDefaultModel();

        for (String service : listFusekiServices())
            try {
                String sparqlURL = String.format(SPARQL_URL_TEMPLATE, service);
                String query = ""
                        + "construct {?s ?p ?o.}\n"
                        + "where {\n"
                        + "  {?s ?p ?o. filter(regex(str(?s), \"/%1$s$\") || regex(str(?o), \"/%1$s$\"))}\n"
                        + "  union\n"
                        + "  {GRAPH ?g {?s ?p ?o. filter(regex(str(?s), \"/%1$s$\") || regex(str(?o), \"/%1$s$\"))}}\n"
                        + "}";
                query = String.format(query, uri);
                QueryExecution q = QueryExecutionFactory.sparqlService(sparqlURL, query);
                q.execConstruct(model);
            } catch (Exception e) {
            }
        return model;
    }

    private static List<String> listFusekiServices() {
        List<String> services = new ArrayList<>();
        List<String> configFiles = new ArrayList<>();
        String fuseki_home = System.getenv("FUSEKI_BASE");
        File dir = new File(fuseki_home + "/configuration");

        try {
            for (File file : dir.listFiles())
                if (file.isFile())
                    configFiles.add(file.getName());
        } catch (Exception e) {
        }
        for (String configFile : configFiles)
            try {
                Model model = ModelFactory.createDefaultModel();
                model.read("file:///" + fuseki_home + "/configuration/" + configFile);
                String query = ""
                        + "prefix fuseki: <http://jena.apache.org/fuseki#>\n"
                        + "select ?name\n"
                        + "where {[] a fuseki:Service;\n"
                        + "          fuseki:name ?name.\n"
                        + "       filter(!regex(?name,\"^temp\") "
                        + "              && !regex(?name,\"^Temp\"))\n"
                        + "}";
                QueryExecution q = QueryExecutionFactory.create(query, model);
                ResultSet result = q.execSelect();
                while (result.hasNext())
                    services.add(result.next().get("name").toString());
            } catch (Exception e) {
            }
        return services;
    }
}
