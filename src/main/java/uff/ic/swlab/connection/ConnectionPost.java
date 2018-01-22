/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author angelo
 */
public class ConnectionPost {
    public static Connection Conectar()  {
        Connection connection = null;
        String jdbcurl = "jdbc:postgresql://localhost:5432/tranning_base";
        String username = "postgres";
        String password = "fluminense";
        try{
            Class.forName("org.postgresql.Driver");
            Connection jdbcConnection = DriverManager.getConnection(jdbcurl, username, password);
            return jdbcConnection;
        }catch (Throwable e){
            System.out.println("Problemas na conexao com o banco de dados." + e);
            return null;
        }
    }
    
}



