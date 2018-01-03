/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.swlab.classifier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author angelo
 */
public class BayesianClassifier {
    
    public static Float GetProbability(String linkset, String dataset, Connection conn) throws SQLException{
        float result = 0;
        String qr = "SELECT prob FROM prob WHERE feature = ? AND dataset = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setString(1, linkset);
        stm.setString(2, dataset);
        ResultSet rs = stm.executeQuery();
        while(rs.next()){
            result = rs.getFloat("prob");
        }
        rs.close();
        stm.close();
        return result;
    }
    
    public static Float GetProbabilityGlobal(String dataset, Connection conn) throws SQLException{
        float result = 0;
        String qr = "SELECT prob FROM prob_global WHERE dataset = ?";
        PreparedStatement stm = conn.prepareStatement(qr);
        stm.setString(1, dataset);
        ResultSet rs = stm.executeQuery();
        while(rs.next()){
            result = rs.getFloat("prob");
        }
        rs.close();
        stm.close();
        return result;
    }
    
}
