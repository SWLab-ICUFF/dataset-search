package uff.ic.swlab.datasetsearch;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class DoSearch extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            String path = request.getPathInfo();
            Lang lang = detectRequestedLang(request.getHeader("Accept"));
            String query = URLDecoder.decode(request.getParameter("q"), "UTF-8");
            System.out.println(query);
            int offset = Integer.parseInt(request.getParameter("offset"));
            int limit = Integer.parseInt(request.getParameter("limit"));

            if (lang == null) {
                String resource = ("" + request.getRequestURL()).replaceFirst("http://", "http/")
                        + "?q=" + URLEncoder.encode(query, "UTF-8") + "&offset=" + offset + "&limit=" + limit;

                String url = "http://linkeddata.uriburner.com/about/html/" + resource + "&@Lookup@=&refresh=clean";
                response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                response.setHeader("Location", url);
            } else
                try (OutputStream httpReponse = response.getOutputStream()) {
                    Model model;
                    if ((new UrlValidator()).isValid(query))

                        model = doVoidBasedSearch(response, new URL(query));
                    else
                        model = doKeywordSearch(response, query);

                    if (model.size() > 0) {
                        response.setContentType(lang.getContentType().getContentType());
                        RDFDataMgr.write(httpReponse, model, lang);
                        httpReponse.flush();
                    } else
                        response.setStatus(HttpServletResponse.SC_NOT_FOUND);

                }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setHeader("Location", "http://localhost:8080/dataset-search/");
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    private Model doVoidBasedSearch(HttpServletResponse response, URL datasetURI) {
        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model doKeywordSearch(HttpServletResponse response, String keywords) {

        String queryString = "prefix dcterms: <http://purl.org/dc/terms/>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix text: <http://jena.apache.org/text#>\n"
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "\n"
                + "construct {?s rdfs:label ?title.}\n"
                + "where {\n"
                + "  graph ?g {?s text:query (dcterms:description '%s') ;\n"
                + "               dcterms:title ?title.}\n"
                + "}";
        queryString = String.format(queryString, keywords);

        Model model = ModelFactory.createDefaultModel();
        QueryExecution q = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", queryString);
        q.execConstruct(model);

        return model;
    }

    private Lang detectRequestedLang(String accept) {
        Lang[] langs = {Lang.RDFXML, Lang.TURTLE, Lang.TTL, Lang.NTRIPLES, Lang.N3, Lang.NT,
            Lang.TRIG, Lang.TRIX, Lang.JSONLD, Lang.RDFJSON, Lang.RDFTHRIFT, Lang.NQUADS, Lang.NQ};
        for (Lang lang : langs)
            if (accept.toLowerCase().contains(lang.getHeaderString().toLowerCase()))
                return lang;
        return null;
    }

}
