/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.tranning;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import uff.ic.swlab.connection.ConnectionPost;
import static uff.ic.swlab.tranning.Bayesian_tranning.InsertProb;

/**
 *
 * @author angelo
 */
public class JRIP_Tranning {

    public static ArrayList<String> getCategory() {
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

    public static ArrayList<String> getCategoryDataset(String dataset) {
        ArrayList<String> categories = new ArrayList<>();
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "        select distinct ?feature \n"
                + "		   from named <" + dataset + ">\n"
                + "                		where {\n"
                + "                  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                                       			?uri_random void:triples ?frequency.\n"
                + "                                				?uri_random <http://purl.org/dc/terms/subject> ?feature \n"
                + "                              		  			optional {?d2 void:triples ?datasetSize} \n"
                + "    }}}";
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
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "                                     select distinct ?d1\n"
                + "                                          where {{graph ?d1 {\n"
                + "      														?d2 void:subset ?uri_random.\n"
                + "      														?uri_random void:triples ?frequency.\n"
                + "      														?uri_random <http://purl.org/dc/terms/subject> ?feature \n"
                + "      														optional {?d2 void:triples ?datasetSize}	\n"
                + "                                                   }}\n"
                + "  											FILTER regex(str(?d1),\"datahub\")\n"
                + "                 \n"
                + "                                                     }";
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

    public static float GetTotalFrequencia(String dataset) {
        float total = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "select (SUM(?frequency) as ?total)\n"
                + "		from named <" + dataset + ">\n"
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
            Literal lit = soln.getLiteral("total");
            total = lit.getFloat();
        }
        qe.close();

        return total;
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
        ResultSet rs = qe.execSelect();
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

    public static float TF(String category, String dataset) {
        float tf = 0;
        int frequency = 0;
        float total_frequencia = GetTotalFrequencia(dataset);
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "   select distinct ?frequency\n"
                + " 		from named <" + dataset + ">\n"
                + "		where {\n"
                + "  				{graph ?d1 {?d2 void:subset ?uri_random.\n"
                + "                       			?uri_random void:triples ?frequency.\n"
                + "                				?uri_random <http://purl.org/dc/terms/subject> <" + category + ">\n"
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
            Literal lit = soln.getLiteral("frequency");
            frequency = lit.getInt();
        }
        qe.close();
        if (frequency == 0) {
            tf = 0;
        } else {
            tf = frequency / total_frequencia;
        }

        return tf;

    }

    public static void ArmazenaTFIDF(String feature, String dataset, float tf_idf, Connection conn) throws ClassNotFoundException, SQLException {
        if (conn != null) {
            String query = "INSERT INTO tf_idf VALUES (?,?,?) ";
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setString(1, feature);
            stm.setString(2, dataset);
            stm.setFloat(3, tf_idf);
            stm.executeUpdate();

        }

    }

    public static void createHeadWeka(String dataset, Map<String, Integer> indices_categories) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(System.getProperty("user.dir")+"/dat/"+dataset+".arff");
        //PrintWriter writer = new PrintWriter("/media/angelo/Novo volume/dat/" + dataset + ".arff");
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
        writer.close();
    }

    public static int getClass(String dataset, String linkset) {
        int result = 0;
        String qr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX void: <http://rdfs.org/ns/void#>\n"
                + "                      select distinct ?d1\n"
                + "						from named <" + dataset + ">\n"
                + "                          where {{graph ?d1 {?d2 void:subset ?ls.\n"
                + "                                      ?ls void:objectsTarget <" + linkset + ">. \n"
                + "                                   }}\n"
                + "                          }";
        
        QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:8080/fuseki/DatasetDescriptions/sparql", qr);
        ResultSet rs = qe.execSelect();
        String aux = null;
        while (rs.hasNext()) {
            QuerySolution soln = rs.nextSolution();
            aux = String.valueOf(soln.get("d1"));
        }
        if(aux != null)
            result = 1;
        else
            result = 0;
        
        qe.close();
        return result;

    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException, UnsupportedEncodingException, IOException, InterruptedException, ExecutionException {
        Map<String, Integer> indices_categories = new HashMap<String, Integer>();
        ArrayList<String> categories = getCategory();
        Integer indice = 0;
        System.out.println("Assigning Indices...");
        for (String category : categories) {
            indices_categories.put(category, indice);
            indice = indice + 1;
        }

        System.out.println("Get All Datasets");
        ArrayList<String> datasets = GetDatasets();
        for (String dataset : datasets) {
            System.out.println(dataset);
            String v_aux[] = dataset.split("/");
            int size = v_aux.length;
            String d = v_aux[size - 1];
            System.out.println("Create head file for Dataset " + d);
            createHeadWeka(d, indices_categories);
        }
        
        System.out.println("Getting Categories");
        Map<String, ArrayList<String>> category_datasets = new HashMap<String, ArrayList<String>>();
        for(String dataset: datasets){
            ArrayList<String> category_dataset = getCategoryDataset(dataset);
            category_datasets.put(dataset, category_dataset);
        }
        
        int counter = 0;
        Float[] result;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Map<String, Float[]> all_vectors = new HashMap<String, Float[]>();
        for (String dataset : datasets) {
            if(dataset.equals("http://datahub.io/api/rest/dataset/rkb-explorer-acm")){
                ArrayList<String> category_dataset = category_datasets.get(dataset);
            System.out.println((++counter) + " Building vector for " + dataset);
            Future <Float[]> result_t = pool.submit(new TaskBuildVector(indices_categories.size(), dataset, category_dataset, datasets, indices_categories));
            all_vectors.put(dataset, result_t.get());
                
            }
            
            
            
        }

        String dir = System.getProperty("user.dir")+"/dat";
        File file = new File(dir);
        File afile[] = file.listFiles();
        for (int j = 0; j < afile.length; j++) {
            File arquivos = afile[j];
            String filename = "http://datahub.io/api/rest/dataset/" + arquivos.getName().replace(".arff", "");
            BufferedWriter bw = new BufferedWriter(new FileWriter(dir + "/" + arquivos.getName(), true));
            System.out.println(filename);
            Set<String> keys = all_vectors.keySet();
            for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
                String key = iterator.next();
                if (!key.equals(filename)) {
                    Float vetor[] = all_vectors.get(key);
                    for (int i = 0; i < vetor.length; i++) {
                        bw.write(String.valueOf(vetor[i]) + ",");
                    }
                    String v_linkset[] = key.split("/");
                    int size = v_linkset.length;
                    String linkset = "http://swlab.ic.uff.br/resource/" + v_linkset[size - 1];
                    int class_ = getClass(filename, linkset);
                    if(class_ == 1)
                        System.out.println(class_);
                        
                    bw.write(String.valueOf(class_));
                    bw.write("\n");

                }

            }

            bw.close();
        }
    }

}
