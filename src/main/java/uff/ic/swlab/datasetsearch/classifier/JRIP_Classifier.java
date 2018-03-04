/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.datasetsearch.classifier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
//import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ModelFactoryBase;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

/**
 *
 * @author angelo
 */
public class JRIP_Classifier {

    public static ArrayList<String> GetDatasets() {
        ArrayList<String> datasets = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "select distinct ?d1\n"
                + "                          where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                                             ?ls void:objectsTarget ?feature.\n"
                + "        									?ls void:triples ?frequency. \n"
                + "                							?d2 void:triples ?datasetSize\n"
                + "                                   }} \n"
                + "                                FILTER regex(str(?d1),\"datahub\")\n"
                + "                                                }";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        org.apache.jena.query.ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String dataset = String.valueOf(soln.get("d1"));
            datasets.add(dataset);
        }
        qe.close();
        return datasets;
    }

    public static Map<String, Integer> getFeatures(Connection conn) throws SQLException {
        Map<String, Integer> indices_categories = new HashMap<String, Integer>();
        String qr = "SELECT id, feature FROM id_categories";
        PreparedStatement stm = conn.prepareStatement(qr);
        ResultSet rs = stm.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String features = rs.getString("feature");
            indices_categories.put(features, id);
        }
        rs.close();
        stm.close();
        return indices_categories;
    }

    public static float idf(String category, ArrayList<String> datasets) {
        int total_datasets = datasets.size();
        float idf = 0;
        int frequency = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "select distinct (COUNT(?d1) as ?total)\n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> <" + category + ">\n"
                + "              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "                		      }}\n"
                + "		\n"
                + "		}\n"
                + "		\n"
                + "   \n"
                + "\n"
                + " ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        org.apache.jena.query.ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit = soln.getLiteral("total");
            frequency = lit.getInt();
        }
        qe.close();
        if (frequency == 0) {
            idf = 0;
        } else {
            idf = frequency / (float) total_datasets;
        }

        return idf;
    }

    public static float TF_test(String feature, int frequency, Model voID) {
        float result = 0;
        int total = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "Select (SUM(?frequency) as ?total)\n"
                + "  where{\n"
                + "    	?d2 void:subset ?uri_random.\n"
                + "    	?uri_random <http://purl.org/dc/terms/subject> ?feature. \n"
                + "   		?uri_random void:triples ?frequency.\n"
                + "  }";
        QueryExecution qe = QueryExecutionFactory.create(qr, voID);
        org.apache.jena.query.ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit = soln.getLiteral("total");
            total = lit.getInt();
        }
        qe.close();
        result = frequency / (float) total;
        return result;
    }

    public static void deleteFile() {
        File file = new File("/home/angelo/Repositorio_Codigos/dataset-search/test/file_test.arff");
        file.delete();
    }

    public static void creatfiletest(Float[] vetor, Map<String, Integer> indices_categories) throws FileNotFoundException {
        //PrintWriter writer = new PrintWriter(System.getProperty("user.dir") + "/test/file_test.arff");
        PrintWriter writer = new PrintWriter("/home/angelo/Repositorio_Codigos/dataset-search/test/file_test.arff");
        writer.print("@RELATION features");
        writer.print("\n\n\n");
        Set<String> key = indices_categories.keySet();
        for (Iterator<String> iterator = key.iterator(); iterator.hasNext();) {
            String chave = iterator.next();
            if (chave.contains(",")) {
                chave = chave.replaceAll(",", "");
            }
            if (chave.contains("'")) {
                chave = chave.replaceAll("'", "");
            }

            writer.print("@ATTRIBUTE " + chave + " " + "REAL");
            writer.print("\n");
        }
        writer.print("@ATTRIBUTE class {0,1}");
        writer.print("\n\n\n");
        writer.print("@Data");
        writer.print("\n");
        for (Float number : vetor) {
            writer.print(String.valueOf(number) + ",");
        }
        writer.print("?");
        writer.print("\n");
        writer.close();

    }

    public static Map<String, Double> Classifier() throws Exception {
        Map<String, Double> rank = new HashMap<>();
        String dir = "/home/angelo/Repositorio_Codigos/dataset-search/dat/";
        String test = "/home/angelo/Repositorio_Codigos/dataset-search/test/file_test.arff";
        ArffLoader loader = new ArffLoader();
        loader.setFile(new File(test));
        loader.getStructure();
        Instances testset = loader.getDataSet();
        testset.setClassIndex(testset.numAttributes() - 1);

        File file = new File(dir);
        File[] list_datasets = file.listFiles();

        for (File file_dataset : list_datasets) {
            System.out.println(file_dataset);
            Classifier cls = (Classifier) weka.core.SerializationHelper.read(file_dataset.toString());
            double[] result = cls.distributionForInstance(testset.get(0));
            String[] v_name = file_dataset.toString().split("/");
            int size = v_name.length;
            String name_dataset = "http://datahub.io/api/rest/dataset/" + v_name[size - 1].replace(".model", "");
            rank.put(name_dataset, result[1]);
        }
        
//        Set<Map.Entry<String, Double>> set = rank.entrySet();
//        List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(set);
//        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
//
//            @Override
//            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
//                return (o2.getValue()).compareTo(o1.getValue());
//            }
//
//        });

        System.out.println("fim");

        return rank;
    }

    public static Model createRank(Map<String, Double> rank, Integer limit, Integer offset) {

        Model modelRank = ModelFactory.createDefaultModel();
        modelRank.read("http://purl.org/voc/vrank");
        modelRank.setNsPrefix("prov", "http://purl.org/voc/vrank#");

        Property hasRank = modelRank.getProperty("http://purl.org/voc/vrank#hasrank");

        Property property_rank = modelRank.getProperty("http://purl.org/voc/vrank#rank");
        Property property_value = modelRank.getProperty("http://purl.org/voc/vrank#hasRankValue");

        Model model = ModelFactory.createDefaultModel();
        Double result;
        Set<String> pair = rank.keySet();
        for (Iterator<String> iterator = pair.iterator(); iterator.hasNext();) {
            String chave = iterator.next();
            Resource resouce = model.createResource(chave)
                    .addProperty(property_rank, hasRank)
                    .addProperty(property_value, model.createTypedLiteral(rank.get(chave)));
        }

        String qr = "PREFIX vrank: <http://purl.org/voc/vrank#>\n"
                + "\n"
                + "CONSTRUCT {?dataset a vrank:hasrank. \n"
                + "  			?dataset vrank:hasRankValue ?score.\n"
                + "} \n"
                + "	WHERE{?dataset vrank:rank ?rank.\n"
                + "  		  ?dataset vrank:hasRankValue ?score.\n"
                + "		  } order by DESC(?score) "
                + (offset != null ? "offset " + offset + "\n" : "")
                + (limit != null ? "limit " + limit + "\n" : "");
        Model model_result = ModelFactory.createDefaultModel();
        QueryExecution q = QueryExecutionFactory.create(qr, model);
        q.execConstruct(model_result);
        return model_result;

    }

}
