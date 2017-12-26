/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.tranning;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import uff.ic.swlab.connection.ConnectionPost;
import static uff.ic.swlab.tranning.Bayesian_tranning.InsertProb;

/**
 *
 * @author angelo
 */
public class JRIP_Tranning {
    
    
    public static ArrayList<String> getCategory(){
        ArrayList<String> categories = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "   select distinct ?feature \n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> ?feature \n"
                + "              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "                		      }}\n"
                + "		\n"
                + "		} ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String category = String.valueOf(soln.get("feature"));
            categories.add(category);   
        }
        qe.close();
        return categories;
        
    }
    
    public static ArrayList<String> GetDatasets() {
        ArrayList<String> datasets = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "     select distinct ?d1\n"
                + "          where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                                      ?ls void:objectsTarget ?feature.\n"
                + "                   }} \n"
                + "                FILTER regex(str(?d1),\"datahub\")\n"
                + "                                }";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String dataset = String.valueOf(soln.get("d1"));
            datasets.add(dataset);
        }
        qe.close();
        return datasets;
    }
    
    public static float GetTotalFrequencia(String dataset){
        float total = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "select (SUM(?frequency) as ?total)\n"
                + "		from named <"+dataset+">\n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> ?feature \n"
                + "              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "                		      }}\n"
                + "		\n"
                + "		} \n"
                + "   ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("total");
            total = lit.getFloat();
        }
        qe.close();
        
        return total;
    }
    
    public static float idf(String category, ArrayList<String> datasets){
        int total_datasets = datasets.size();
        float idf = 0;
        int frequency = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "select distinct (COUNT(?d1) as ?total)\n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> <"+category+">\n"
                + "              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "                		      }}\n"
                + "		\n"
                + "		}\n"
                + "		\n"
                + "   \n"
                + "\n"
                + " ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("total");
            frequency = lit.getInt();
        }
        qe.close();
        if(frequency == 0)
            idf = 0;
        else
            idf = frequency / (float) total_datasets;
            
        return idf;
    }
    
    public static float TF(String category, String dataset){
        float tf = 0;
        int frequency = 0;
        float total_frequencia = GetTotalFrequencia(dataset);
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "   select distinct ?frequency\n"
                + " 		from named <"+dataset+">\n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> <"+category+">\n"
                + "              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "                		      }}\n"
                + "		\n"
                + "		}\n"
                + "   \n"
                + "\n"
                + " ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("frequency");
            frequency = lit.getInt();
        }
        qe.close();
        if(frequency == 0)
            tf = 0;
        else
            tf = frequency / total_frequencia;
            
        return tf;
        
    }
    
    
    public static void ArmazenaTFIDF(String feature, String dataset, float tf_idf, Connection conn) throws ClassNotFoundException, SQLException{
        if (conn != null) {
            String query = "INSERT INTO tf_idf VALUES (?,?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setString(1, feature);
            stm.setString(2, dataset);
            stm.setFloat(3, tf_idf);
            stm.executeUpdate();
            
        }
        
        
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        System.out.println("Calculing tf-idf...");
        Connection conn = ConnectionPost.Conectar();
        ArrayList<String> categories = getCategory();
        ArrayList<String> datasets = GetDatasets();
        Map<List<String>, Float> all_tf_idf = new HashMap<List<String>, Float>();
        for(String category: categories){
            System.out.println(category);
            for(String dataset: datasets){
                float value_tf = TF(category, dataset);
                float value_idf = idf(category, datasets);
                float value_tf_idf = value_tf * value_idf;
                
                List<String> features = new ArrayList<>();
                features.add(category);
                features.add(dataset);
                all_tf_idf.put(features, value_tf_idf);
            }
        }
        Set<List<String>> chaves = all_tf_idf.keySet();
        for (Iterator<List<String>> iterator = chaves.iterator(); iterator.hasNext();){
            List<String> chave = iterator.next();
            ArmazenaTFIDF(chave.get(0),  chave.get(1), all_tf_idf.get(chave), conn);
        }
        
        conn.close();
    }
  
    
}
