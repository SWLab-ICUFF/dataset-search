package uff.ic.swlab.datasetsearch;

import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
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

            String q = URLDecoder.decode(request.getParameter("q"), "UTF-8");
            URL voidURL = null;
            String keywords = null;
            if ((new UrlValidator()).isValid(q))
                voidURL = new URL(q);
            else
                keywords = q;

            String offsetString = request.getParameter("offset");
            Integer offset = offsetString != null ? Integer.parseInt(offsetString) : null;

            String limitString = request.getParameter("limit");
            Integer limit = limitString != null ? Integer.parseInt(limitString) : null;

            if (keywords != null)
                if (lang == null) {
                    String resource = ("" + request.getRequestURL()).replaceFirst("http://", "http/")
                            + "?q=" + URLEncoder.encode(q, "UTF-8")
                            + (offset != null ? "&offset=" + offset : "")
                            + (limit != null ? "&limit=" + limit : "");
                    String url = "http://linkeddata.uriburner.com/about/html/" + resource + "&@Lookup@=&refresh=clean";
                    response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                    response.setHeader("Location", url);
                } else
                    try (OutputStream httpReponse = response.getOutputStream()) {
                        Model model;
                        if (voidURL != null)
                            model = doVoidBasedSearch(voidURL, offset, limit);
                        else
                            model = doKeywordSearch(keywords, offset, limit);

                        if (model.size() > 0) {
                            response.setContentType(lang.getContentType().getContentType());
                            RDFDataMgr.write(httpReponse, model, lang);
                            httpReponse.flush();
                        } else
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    }
            else
                System.out.println("tratar void");
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    private Model doVoidBasedSearch(URL datasetURI, Integer offset, Integer limit) {
        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model doKeywordSearch(String keywords, Integer offset, Integer limit) {

        String queryString = "prefix dcterms: <http://purl.org/dc/terms/>\n"
                + "prefix void: <http://rdfs.org/ns/void#>\n"
                + "prefix text: <http://jena.apache.org/text#>\n"
                + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "\n"
                + "construct {?s a void:Dataset.\n"
                + "           ?s rdfs:label ?title.}\n"
                + "where {\n"
                + "  graph ?g {?s text:query (dcterms:description '%s') ;\n"
                + "               dcterms:title ?title.}\n"
                + "}\n"
                + (offset != null ? "offset " + offset + "\n" : "")
                + (limit != null ? "limit " + limit + "\n" : "");
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
