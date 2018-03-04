/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.datasetsearch.classifier;

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
        String dir = "/home/angelo/teste/";
        File file = new File(dir);
        File afile[] = file.listFiles();

        for (int j = 0; j < afile.length; j++) {
            File arquivos = afile[j];
            System.out.println(arquivos.toString());
            ArffLoader loader = new ArffLoader();
            loader.setFile(new File(arquivos.toString())); 
            loader.getStructure();
            
            String[] vetor = arquivos.toString().split("/");
            int size = vetor.length;
            String name = vetor[size -1].replace(".arff", "");
            
            
            Instances trainingset = loader.getDataSet();
            int classIndex = trainingset.numAttributes() - 1;
            trainingset.setClassIndex(classIndex);
            
            JRip jrip = new JRip();
            jrip.buildClassifier(trainingset);
            weka.core.SerializationHelper.write(dir_rules+"/"+name+".model", jrip);
            
        }
        

    }
    
}
