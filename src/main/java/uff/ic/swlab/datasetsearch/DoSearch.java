package uff.ic.swlab.datasetsearch;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.VCARD;
import uff.ic.swlab.connection.ConnectionPost;
import uff.ic.swlab.tranning.Bayesian_tranning;
import uff.swlab.classifier.BayesianClassifier;

public class DoSearch extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            Parameters p = new Parameters(request);
            if (p.isKeywordSearch()) {
                if (p.isHumanRequest()) {
                    redirectToUserInterface(request, p, response);
                } else if (p.isAppRequest()) {
                    try (OutputStream httpReponse = response.getOutputStream()) {
                        Model model = doKeywordSearch(p.keywords, p.offset, p.limit);
                        if (model.size() > 0) {
                            response.setContentType(p.lang.getContentType().getContentType());
                            RDFDataMgr.write(httpReponse, model, p.lang);
                            httpReponse.flush();
                        } else {
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                        }
                    }
                } else {
                    new Exception("Unknown request agent.");
                }
            } else if (p.isVoidSearch()) {
                if (p.isHumanRequest()) {
                    redirectToUserInterface(request, p, response);
                } else if (p.isAppRequest()) {
                    if (p.method == p.method.SELECT) {
                        try (OutputStream httpReponse = response.getOutputStream()) {
                            Model model = doVoidSearchForSelect(p.voidURL, p.offset, p.limit);
                            if (model.size() > 0) {
                                response.setContentType(p.lang.getContentType().getContentType());
                                RDFDataMgr.write(httpReponse, model, p.lang);
                                httpReponse.flush();
                            } else {
                                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            }
                        }
                    } else if (p.method == p.method.SCAN) {
                        try (OutputStream httpReponse = response.getOutputStream()) {
                            Model model = doVoidSearchForScan(p.voidURL, p.offset, p.limit);
                            if (model.size() > 0) {
                                response.setContentType(p.lang.getContentType().getContentType());
                                RDFDataMgr.write(httpReponse, model, p.lang);
                                httpReponse.flush();
                            } else {
                                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            }
                        }
                    } else {
                        throw new Exception("Unknown search method.");
                    }
                } else {
                    throw new Exception("Unknown request agent.");
                }
            } else {
                throw new Exception("Unknown search type.");
            }
        } catch (Exception e) {
            try {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            } catch (IOException ex) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
        }
    }

    private void redirectToUserInterface(HttpServletRequest request, Parameters p, HttpServletResponse response) throws UnsupportedEncodingException {
        String resource = ("" + request.getRequestURL()).replaceFirst("http://", "http/")
                + "?q=" + URLEncoder.encode(p.query, "UTF-8")
                + (p.offset != null ? "&offset=" + p.offset : "")
                + (p.limit != null ? "&limit=" + p.limit : "")
                + (p.method != null ? "&method=" + p.method.label : "");
        String url = "http://linkeddata.uriburner.com/about/html/" + resource + "&@Lookup@=&refresh=clean";
        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
        response.setHeader("Location", url);
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

    private Model doVoidSearchForSelect(URL voidURL, Integer offset, Integer limit) throws SQLException {
        ArrayList<String> features = new ArrayList<>();
        Connection conn = ConnectionPost.Conectar();
        Model voID = readVoidURL(voidURL);
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "                      select distinct  ?linkset\n"
                + "                          where {?d2 void:subset ?ls.\n"
                + "                                      ?ls void:objectsTarget ?linkset. \n"
                + "                                   \n"
                + "                          }";
        QueryExecution qe = QueryExecutionFactory.create(qr, voID);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String feature = String.valueOf(soln.get("linkset"));
            String[] v_aux = feature.split("/");
            int size = v_aux.length;
            String feature_ = "http://swlab.ic.uff.br/resource/" + v_aux[size - 1];
            features.add(feature_);
        }
        qe.close();
        ArrayList<String> datasets = Bayesian_tranning.GetDatasets();
        Map<String, Double> rank = new HashMap<String, Double>();
        for (String dataset : datasets) {
          //  if (dataset.equals("http://datahub.io/api/rest/dataset/rkb-explorer-acm")) {
                double somatorio = 0;
                double result_dataset = 0;
                double log_prob_global = 0;
                int flag = 0;
                float prob_global = BayesianClassifier.GetProbabilityGlobal(dataset, conn);
                try {
                    log_prob_global = Math.log(prob_global);
                } catch (Throwable e) {
                    result_dataset = Double.NEGATIVE_INFINITY;
                    System.out.println(result_dataset);
                    flag = 1;
                }
                if (flag == 0) {
                    for (String feature : features) {
                        float prob = BayesianClassifier.GetProbability(feature, dataset, conn);
                        double result_log = 0;
                        try {
                            result_log = Math.log(prob);
                        } catch (Throwable e) {
                            //result_log = Double.NEGATIVE_INFINITY; //de 20 linksets 19 tem probabilidades e 1 não tem....
                            continue;
                        }
                        somatorio = somatorio + result_log;
                    }
                    result_dataset = somatorio + log_prob_global;
                    System.out.println(result_dataset);

                }
          //  }

        }
        conn.close();
        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model doVoidSearchForScan(URL voidURL, Integer offset, Integer limit) {
        Model voID = readVoidURL(voidURL);

        Model model = ModelFactory.createDefaultModel();
        return model;
    }

    private Model readVoidURL(URL voidURL) {
        Model aux = ModelFactory.createDefaultModel();
        try {
            Lang[] langs = {null, Lang.TURTLE, Lang.RDFXML, Lang.NTRIPLES, Lang.TRIG,
                Lang.NQUADS, Lang.JSONLD, Lang.RDFJSON, Lang.TRIX, Lang.RDFTHRIFT, Lang.NQ, Lang.N3, Lang.NT, Lang.TTL};
            for (Lang lang : langs) {
                try {
                    RDFDataMgr.read(aux, voidURL.toString(), lang);
                    return aux;
                } catch (Throwable e) {
                    continue;
                }
            }
        } catch (Throwable e) {
            System.out.println("Error Read Void.");
        }
        return aux;
    }

}
