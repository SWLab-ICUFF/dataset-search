package uff.ic.swlab.datasetsearch;

import uff.ic.swlab.utils.DBConnection;
import uff.ic.swlab.utils.Params;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import uff.ic.swlab.datasetsearch.classifier.BayesianClassifier;
import static uff.ic.swlab.datasetsearch.classifier.BayesianClassifier.GetFeatures;
import static uff.ic.swlab.datasetsearch.classifier.BayesianClassifier.GetIndices;
import uff.ic.swlab.datasetsearch.classifier.JRIP_Classifier;

public class DoSearch extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            Params p = new Params(request);
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

    private void redirectToUserInterface(HttpServletRequest request, Params p, HttpServletResponse response) throws UnsupportedEncodingException {
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

    private Model doVoidSearchForSelect(URL voidURL, Integer offset, Integer limit) throws SQLException, IOException {
        Map<String, Double> rank = new HashMap<>();
        ArrayList<String> features = new ArrayList<>();
        Connection conn = DBConnection.connect();
        Model voID = readVoidURL(voidURL);

        Map<String, Integer> indices_datasets = GetIndices(conn);
        Map<String, Integer> indices_features = GetFeatures(conn);

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

        Set<String> datasets = indices_datasets.keySet();
        for (Iterator<String> iterator = datasets.iterator(); iterator.hasNext();) {
            String chave = iterator.next();
            double somatorio = 0;
            double result_dataset = 0;
            double log_prob_global = 0;
            int flag = 0;
            float prob_global = BayesianClassifier.GetProbabilityGlobal(indices_datasets.get(chave), conn);
            try {
                log_prob_global = Math.log(prob_global);
            } catch (Throwable e) {
                result_dataset = Double.NEGATIVE_INFINITY;
                rank.put(chave, 0.0);
                flag = 1;
            }
            if (flag == 0) {
                for (String feature : features) {
                    float prob = BayesianClassifier.GetProbability(indices_features.get(feature), indices_datasets.get(chave), conn);
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
                rank.put(chave, result_dataset);
            }
        }
        conn.close();

        Model model_result = BayesianClassifier.CreateRank(rank, limit, offset);
        return model_result;
    }

    private Model doVoidSearchForScan(URL voidURL, Integer offset, Integer limit) throws FileNotFoundException, Exception {
        Model voID = readVoidURL(voidURL);
        Connection conn = DBConnection.connect();
        Integer indice = 0;
        Map<String, Integer> indices_categories = JRIP_Classifier.getFeatures(conn);
        ArrayList<String> datasets = JRIP_Classifier.GetDatasets();

        Float[] vetor = new Float[indices_categories.size()];
        Arrays.fill(vetor, new Float(0));

        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "Select distinct ?feature ?frequency\n"
                + "  where{\n"
                + "    	?d2 void:subset ?uri_random.\n"
                + "    	?uri_random <http://purl.org/dc/terms/subject> ?feature. \n"
                + "   		?uri_random void:triples ?frequency.\n"
                + "  }";
        QueryExecution qe = QueryExecutionFactory.create(qr, voID);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String feature = String.valueOf(soln.get("feature"));
            Literal lit = soln.getLiteral("frequency");
            int frequency = lit.getInt();
            float value_tf = JRIP_Classifier.TF_test(feature, frequency, voID);
            float value_idf = JRIP_Classifier.idf(feature, datasets);
            float value_tf_idf = value_tf * value_idf;
            try {
                int index = indices_categories.get(feature);
                vetor[index] = value_tf_idf;
            } catch (Throwable e) {
                continue;
            }

        }
        qe.close();
        JRIP_Classifier.creatfiletest(vetor, indices_categories);
        Map<String, Double> rank = JRIP_Classifier.Classifier();
        JRIP_Classifier.deleteFile();

        //Model model = createDefaultModel();
        Model model = JRIP_Classifier.createRank(rank, limit, offset);
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
