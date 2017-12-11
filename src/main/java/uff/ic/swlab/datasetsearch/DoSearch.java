package uff.ic.swlab.datasetsearch;

import java.io.OutputStream;
import java.net.URL;
import java.net.URLEncoder;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;

public class DoSearch extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            Parameters p = new Parameters(request);

            if (p.isKeywordSearch())
                if (!p.isSearchForSelect()) {
                    String resource = ("" + request.getRequestURL()).replaceFirst("http://", "http/")
                            + "?q=" + URLEncoder.encode(p.query, "UTF-8")
                            + (p.offset != null ? "&offset=" + p.offset : "")
                            + (p.limit != null ? "&limit=" + p.limit : "")
                            + (p.method != null ? "&method=" + p.method : "");
                    String url = "http://linkeddata.uriburner.com/about/html/" + resource + "&@Lookup@=&refresh=clean";
                    response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                    response.setHeader("Location", url);
                } else
                    try (OutputStream httpReponse = response.getOutputStream()) {
                        Model model = doKeywordSearch(p.keywords, p.offset, p.limit);

                        if (model.size() > 0) {
                            response.setContentType(p.lang.getContentType().getContentType());
                            RDFDataMgr.write(httpReponse, model, p.lang);
                            httpReponse.flush();
                        } else
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    }
            else if (p.isVoidSearch()) {
                String resource = ("" + request.getRequestURL()).replaceFirst("http://", "http/")
                        + "?q=" + URLEncoder.encode(p.query, "UTF-8")
                        + (p.offset != null ? "&offset=" + p.offset : "")
                        + (p.limit != null ? "&limit=" + p.limit : "")
                        + (p.method != null ? "&method=" + p.method : "");
                String url = "http://linkeddata.uriburner.com/about/html/" + resource + "&@Lookup@=&refresh=clean";
                response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                response.setHeader("Location", url);

                if (!p.isSearchForSelect())
                    try (OutputStream httpReponse = response.getOutputStream()) {
                        Model model = doVoidSearchForSelect(p.voidURL, p.offset, p.limit);

                        if (model.size() > 0) {
                            response.setContentType(p.lang.getContentType().getContentType());
                            RDFDataMgr.write(httpReponse, model, p.lang);
                            httpReponse.flush();
                        } else
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    }
                else
                    try (OutputStream httpReponse = response.getOutputStream()) {
                        Model model = doVoidSearchForScan(p.voidURL, p.offset, p.limit);

                        if (model.size() > 0) {
                            response.setContentType(p.lang.getContentType().getContentType());
                            RDFDataMgr.write(httpReponse, model, p.lang);
                            httpReponse.flush();
                        } else
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    }
            } else
                throw new Exception("Undefined search method");
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
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

    private Model doVoidSearchForSelect(URL voidURL, Integer offset, Integer limit) {
        Model voID = readVoidURL(voidURL);
        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model doVoidSearchForScan(URL voidURL, Integer offset, Integer limit) {
        Model voID = readVoidURL(voidURL);
        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model readVoidURL(URL voidURL) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
