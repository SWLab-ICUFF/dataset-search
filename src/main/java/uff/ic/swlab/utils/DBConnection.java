/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.utils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 *
 * @author angelo
 */
public class DBConnection {

    public static Connection connect() {
        Connection connection = null;
        String jdbcurl = "jdbc:postgresql://localhost:5432/tranning_base";
        String username = "postgres";
        String password = "123";
        try {
            Class.forName("org.postgresql.Driver");
            Connection jdbcConnection = DriverManager.getConnection(jdbcurl, username, password);
            return jdbcConnection;
        } catch (Throwable e) {
            System.out.println("Problemas na conexao com o banco de dados." + e);
            return null;
        }
    }

}
