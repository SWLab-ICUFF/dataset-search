package uff.ic.swlab.datasetsearch.classifier;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author angelo
 */
public class BayesianClassifier {

    public static Float GetProbability(int linkset, int dataset, Connection conn) throws SQLException {
        float result = 0;
        String qr = "SELECT prob FROM prob WHERE feature = ? AND dataset = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setInt(1, linkset);
        stm.setInt(2, dataset);
        ResultSet rs = stm.executeQuery();
        while (rs.next())
            result = rs.getFloat("prob");
        rs.close();
        stm.close();
        return result;
    }

    public static Float GetProbabilityGlobal(int dataset, Connection conn) throws SQLException {
        float result = 0;
        String qr = "SELECT prob FROM prob_global WHERE dataset = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setInt(1, dataset);
        ResultSet rs = stm.executeQuery();
        while (rs.next())
            result = rs.getFloat("prob");
        rs.close();
        stm.close();
        return result;
    }

    public static Map<String, Integer> GetIndices(Connection conn) throws SQLException {
        Map<String, Integer> indices_datasets = new HashMap<String, Integer>();
        String qr = "SELECT id, dataset FROM id_dataset";
        PreparedStatement stm = conn.prepareStatement(qr);
        java.sql.ResultSet rs = stm.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String dataset = rs.getString("dataset");
            indices_datasets.put(dataset, id);

        }
        stm.close();
        rs.close();
        return indices_datasets;
    }

    public static Map<String, Integer> GetFeatures(Connection conn) throws SQLException {
        Map<String, Integer> indices_features = new HashMap<String, Integer>();
        String qr = "SELECT id, feature FROM id_feature";
        PreparedStatement stm = conn.prepareStatement(qr);
        java.sql.ResultSet rs = stm.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String dataset = rs.getString("feature");
            indices_features.put(dataset, id);

        }
        stm.close();
        rs.close();
        return indices_features;
    }

    public static Model CreateRank(Map<String, Double> rank, Integer limit, Integer offset) throws FileNotFoundException {
        Set<Entry<String, Double>> set = rank.entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {

            @Override
            public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
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

            int t = java.lang.Double.compare(pair.getValue(), Double.NEGATIVE_INFINITY);
            if (t == 0)
                result = -Double.MAX_VALUE;
            else
                result = pair.getValue();
            Resource resouce = model.createResource(pair.getKey())
                    .addProperty(property_rank, hasRank)
                    .addProperty(property_value, model.createTypedLiteral(result));
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
