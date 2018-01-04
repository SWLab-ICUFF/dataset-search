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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import uff.ic.swlab.connection.ConnectionPost;

/**
 *
 * @author angelo
 */
public class Bayesian_tranning {


    public static ArrayList<String> GetAllLinksets() {
        ArrayList<String> all_ls = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "  PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "                     select distinct ?feature\n"
                + "                          where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                                     ?ls void:objectsTarget ?feature. \n"
                + "        									?ls void:triples ?frequency. \n"
                + "                							?d2 void:triples ?datasetSize\n"
                + "                                   }}\n"
                + "                                }";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String feature = String.valueOf(soln.get("feature"));
            all_ls.add(feature);

        }
        qe.close();
        return all_ls;

    }

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
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String dataset = String.valueOf(soln.get("d1"));
            datasets.add(dataset);
        }
        qe.close();
        return datasets;
    }
    
    public static List<String> GetSet(String linkset){
        List<String> set = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "\n"
                + "SELECT distinct ?d1\n"
                + "where {{graph ?d1 {?d2 void:subset ?ls. \n"
                + "      		?ls void:objectsTarget <"+linkset+">. \n"
                + "   			}}\n"
                + "		   FILTER regex(str(?d1), \"datahub\")\n"
                + "} ";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            String dataset = String.valueOf(soln.get("d1"));
            set.add(dataset);
        }
        qe.close();
        return set;
        
        
    }
    
    public static int VerificaFeature(List<String> dataset_set, String linkset){
        List<String> list_aux  = new ArrayList<>();
        for (String link: dataset_set){
             String aux = "<"+link+">";
             list_aux.add(aux);
        }
        String[] d2 = list_aux.toArray(new String[0]);
        String lista = Arrays.toString(d2).replaceAll("\\[", "\\(").replaceAll("\\]", "\\)");
        
        
        int result = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "      select distinct (COUNT(*) AS ?count)\n"
                + "          where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                      ?ls void:objectsTarget <"+linkset+">. \n"
                + "                   }}\n"
                 + "  filter (?d1 in %1$s)}";
        qr = String.format(qr, lista);
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("count");
            result = lit.getInt();
        }
       qe.close();
       return result;
    }
    
    public static void InsertProb(int feature, int dataset, double prob, Connection conn) throws ClassNotFoundException, SQLException{
        if (conn != null) {
            String query = "INSERT INTO prob VALUES (?,?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setInt(1, feature);
            stm.setInt(2, dataset);
            stm.setDouble(3, prob);
            stm.executeUpdate();
            stm.close();
        }
        
        
    }
    
    public static void InsertProbContext(int dataset, double prob, Connection conn) throws ClassNotFoundException, SQLException{
        if (conn != null) {
            String query = "INSERT INTO prob_global VALUES (?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setInt(1, dataset);
            stm.setDouble(2, prob);
            stm.executeUpdate();
            stm.close();
        }
        
    }
    
    public static void InsertIndiceDataset(String dataset, int id, Connection conn) throws SQLException{
        if (conn != null) {
            String query = "INSERT INTO id_dataset VALUES (?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setInt(1, id);
            stm.setString(2, dataset);
            stm.executeUpdate();
            stm.close();
        }
    }
     public static void InsertIndiceFeature(String feature, int id, Connection conn) throws SQLException{
        if (conn != null) {
            String query = "INSERT INTO id_feature VALUES (?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setInt(1, id);
            stm.setString(2, feature);
            stm.executeUpdate();
            stm.close();
        }
    }
    
    public static int CountLS(String dataset) {
        int result = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "                select distinct (COUNT(?feature) AS ?count)\n"
                + "from named <"+dataset+">\n"
                + "                         where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                                     ?ls void:objectsTarget ?feature.\n"
                + "                                  }}\n"
                + "                 }";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("count");
            result = lit.getInt();
        }
        qe.close();
        return result;

    }
    
    public static int CountConexao(String ls){
        int result = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + " select distinct (COUNT(?d1) AS ?count)\n"
                + "               where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                        ?ls void:objectsTarget <"+ls+">\n"
                + "                     }}\n"
                + "                }";
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            Literal lit= soln.getLiteral("count");
            result = lit.getInt();
        }
        qe.close();
        return result;
    }
    public static int GetIndexDataset(String dataset, Connection conn) throws SQLException{
        int result = 0;
        String qr = "SELECT id FROM id_dataset WHERE dataset = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setString(1, dataset);
        java.sql.ResultSet rs = stm.executeQuery();
        while(rs.next()){
            result = rs.getInt("id");
        }
        rs.close();
        stm.close();
        return result;
    }
     public static int GetIndexFeature(String feature, Connection conn) throws SQLException{
        int result = 0;
        String qr = "SELECT id FROM id_feature WHERE feature = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setString(1, feature);
        java.sql.ResultSet rs = stm.executeQuery();
        while(rs.next()){
            result = rs.getInt("id");
        }
        rs.close();
        stm.close();
        return result;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn = ConnectionPost.Conectar();
        System.out.println("Create indices");
        ArrayList<String> datasets = GetDatasets();
        int id = 1;
        for(String dataset: datasets){
            InsertIndiceDataset(dataset, id, conn);
            id = id + 1;
        }
        ArrayList<String> all_linksets = GetAllLinksets();
        id = 1;
        for(String feature: all_linksets){
            InsertIndiceFeature(feature, id, conn);
            id = id + 1;
        }
        System.out.println("Getting Indices");
        Map<String, Integer> indices_dataset = new HashMap<String, Integer>();
        for(String dataset: datasets){
            int index = GetIndexDataset(dataset, conn);
            indices_dataset.put(dataset, index);
        }
        Map<String, Integer> indices_feature = new HashMap<String, Integer>();
        for(String feature: all_linksets){
            int index = GetIndexFeature(feature, conn);
            indices_feature.put(feature, index);
        }
        
        System.out.println("Calculing probabilities...");
        
        Map<List<String>, Double> all_probabilities = new HashMap<List<String>, Double>();
        for (String linkset : all_linksets) {
            System.out.println("Calculing probabilities feature"+linkset);
            Map<List<String>, Integer> hm = new HashMap<List<String>, Integer>();
            float denominador = 0;
            for (String dataset : datasets) {
                String[] v_aux = dataset.split("/");
                int size = v_aux.length;
                String aux = "http://swlab.ic.uff.br/resource/"+v_aux[size -1];
                List<String> set_datasets = GetSet(aux);
                int numerador = VerificaFeature(set_datasets, linkset);
                denominador = denominador + numerador;
                List<String> named_prob = new ArrayList<>();
                named_prob.add(linkset);
                named_prob.add(dataset);
                hm.put(named_prob, numerador);
            }
            
            Set<List<String>> chaves = hm.keySet();
            for (Iterator<List<String>> iterator = chaves.iterator(); iterator.hasNext();){
                List<String> chave = iterator.next();
                double prob = 0;
                if(hm.get(chave) != 0)
                    prob = hm.get(chave) / denominador;
                else
                    prob = 0;
                
                List<String> features = new ArrayList<>();
                features.add(chave.get(0));
                features.add(chave.get(1));
                all_probabilities.put(features, prob);
                //InsertProb(chave.get(0), chave.get(1), prob, conn);
            }
        }
        Set<List<String>> chaves = all_probabilities.keySet();
        for (Iterator<List<String>> iterator = chaves.iterator(); iterator.hasNext();){
            List<String> chave = iterator.next();
            int index_feature = indices_feature.get(chave.get(0));
            int index_dataset = indices_dataset.get(chave.get(1));
            InsertProb(index_feature, index_dataset, all_probabilities.get(chave), conn);
        }
            
        
        float demoninador_prob_global = 0;
        for (String dataset : datasets) {
            int result = CountLS(dataset);
            demoninador_prob_global = demoninador_prob_global + result;
        }
        
        for (String dataset: datasets){
            String[] v_aux = dataset.split("/");
            int size = v_aux.length;
            String aux = "http://swlab.ic.uff.br/resource/"+v_aux[size -1];
            int numerador_prob_global = CountConexao(aux);
            double prob = 0;
            if(numerador_prob_global != 0)
                prob = numerador_prob_global / demoninador_prob_global;
            else
                prob = 0;
            int index_dataset = indices_dataset.get(dataset);
            InsertProbContext(index_dataset,prob,conn);
        }
        conn.close();
    }
}
