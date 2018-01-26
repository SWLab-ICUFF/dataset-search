/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.swlab.classifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
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
    public static float TF_test(String feature, int frequency, Model voID){
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
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit = soln.getLiteral("total");
            total = lit.getInt();
        }
        qe.close();
        result = frequency / (float) total;
        return result;
    }
    
    public static void deleteFile(){
        File file = new File("/home/angelo/Repositorio_Codigos/dataset-search/test/file_test.arff");
        file.delete();
    }
    
    public static void creatfiletest(Float[] vetor, Map<String, Integer> indices_categories) throws FileNotFoundException{
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
        for(Float number: vetor){
            writer.print(String.valueOf(number)+ ",");
        }
        writer.print("?");
        writer.print("\n");
        writer.close();
        
    }
   
    
    public static Map<String, Double> Classifier() throws Exception{
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
        for(File file_dataset: list_datasets){
            String[] v_name = file_dataset.toString().split("/");
            int size = v_name.length;
            String name_dataset = "http://datahub.io/api/rest/dataset/"+v_name[size - 1].replace(".model", "");
            Classifier cls = (Classifier) weka.core.SerializationHelper.read(file_dataset.toString());
            for (Instance instance : testset) {
                 double[] result = cls.distributionForInstance(instance);
                 rank.put(name_dataset, result[1]);
            }
        }
        return rank;
    }
    
    public static Model createRank(Map<String, Double> rank, Integer limit, Integer offset){
        Set<Map.Entry<String, Double>> set = rank.entrySet();
        List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {

            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }

        });
        Model modelRank = ModelFactory.createDefaultModel();
        modelRank.read("http://purl.org/voc/vrank");
        modelRank.setNsPrefix("prov", "http://purl.org/voc/vrank#");
        
        Property hasRank = modelRank.getProperty("http://purl.org/voc/vrank#hasrank");
        
        Property property_rank = modelRank.getProperty("http://purl.org/voc/vrank#rank");
        Property property_value = modelRank.getProperty("http://purl.org/voc/vrank#hasRankValue");
        
        Model model = ModelFactory.createDefaultModel();
        Double result;
        
        for (Map.Entry<String, Double> pair : list) {
               Resource resouce = model.createResource(pair.getKey())
                    .addProperty(property_rank, hasRank)
                    .addProperty(property_value, model.createTypedLiteral(pair.getValue()));
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
