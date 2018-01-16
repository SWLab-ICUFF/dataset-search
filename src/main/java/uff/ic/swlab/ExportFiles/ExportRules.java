/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.ExportFiles;

import java.io.File;
import weka.classifiers.rules.JRip;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

/**
 *
 * @author angelo
 */
public class ExportRules {
    
    public static void main(String[] args) throws Exception {
        String dir = System.getProperty("user.dir") + "/dat";
        File file = new File(dir);
        File afile[] = file.listFiles();
        for (int j = 0; j < afile.length; j++) {
            File arquivos = afile[j];
            ArffLoader loader = new ArffLoader();
            loader.setFile(new File(arquivos.toString())); // verificar o caminho
            loader.getStructure();
            
            Instances trainingset = loader.getDataSet();
            int classIndex = trainingset.numAttributes() - 1;
            trainingset.setClassIndex(classIndex);
            
            JRip jrip = new JRip();
            jrip.buildClassifier(trainingset);
            weka.core.SerializationHelper.write(dir+arquivos+"/jrip.model", jrip);
        }
    }
    
}
