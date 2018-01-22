/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.swlab.classifier;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static java.util.concurrent.TimeUnit.DAYS;
import weka.classifiers.rules.JRip;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

/**
 *
 * @author angelo
 */
public class ExportRules {
    
    public static void main(String[] args) throws Exception {
        String dir_rules = System.getProperty("user.dir") + "/dat";
        String dir = "/home/angelo/Repositorio_Codigos/dataset-search/files";
        File file = new File(dir);
        File afile[] = file.listFiles();
        ExecutorService pool = Executors.newWorkStealingPool(2);
        for (int j = 0; j < afile.length; j++) {
            File arquivos = afile[j];
            pool.submit(new ExportRulesTask(dir_rules, arquivos));
        }
        
        pool.shutdown();
        System.out.println("Waiting for remaining tasks...");
        pool.awaitTermination(1, DAYS);
    }
    
}
