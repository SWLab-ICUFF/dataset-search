package uff.ic.swlab.datasetsearch;

import java.io.IOException;
import java.net.URL;
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

public class DoSearch extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {

            String query = request.getParameter("q");
            int offset = Integer.parseInt(request.getParameter("offset"));
            int limit = Integer.parseInt(request.getParameter("limit"));

            if ((new UrlValidator()).isValid(query))
                doVoidBasedSearch(response, new URL(query));
            else
                doKeywordSearch(response, query);

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

    private void doVoidBasedSearch(HttpServletResponse response, URL datasetURI) {
        response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

    private void doKeywordSearch(HttpServletResponse response, String keywords) {
        String queryString = "prefix dcterms: <http://purl.org/dc/terms/>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix text: <http://jena.apache.org/text#>\n"
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "\n"
                + "construct {?s a void:Dataset.\n"
                + "           ?s rdfs:label ?title.\n"
                + "           ?s dcterms:description ?description.\n"
                + "           ?s rdf:seeAlso ?seeAlso.}\n"
                + "where {\n"
                + "  graph ?g {?s text:query (dcterms:description 'brazil') ;\n"
                + "               dcterms:title ?title;\n"
                + "               dcterms:description ?description;\n"
                + "               rdfs:seeAlso ?seeAlso.}\n"
                + "}";

        Model model = ModelFactory.createDefaultModel();
        QueryExecution q = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", queryString);
        q.execConstruct(model);

        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
        response.setHeader("Location", "http://localhost:8080/dataset-search/");
    }

    private Lang detectRequestedLang(String accept) {
        Lang[] langs = {Lang.RDFXML, Lang.TURTLE, Lang.TTL, Lang.TRIG, Lang.TRIX, Lang.JSONLD, Lang.RDFJSON,
            Lang.RDFTHRIFT, Lang.NQUADS, Lang.NQ, Lang.NTRIPLES, Lang.N3, Lang.NT};
        for (Lang lang : langs)
            if (accept.toLowerCase().contains(lang.getHeaderString().toLowerCase()))
                return lang;
        return null;
    }

}
