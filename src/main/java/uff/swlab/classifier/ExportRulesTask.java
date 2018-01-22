/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.swlab.classifier;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import weka.classifiers.rules.JRip;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

/**
 *
 * @author angelo
 */
public class ExportRulesTask implements Runnable {

    private String dir;
    private File arquivos;

    public ExportRulesTask(String dir, File arquivos) {
        this.dir = dir;
        this.arquivos = arquivos;
    }

    @Override
    public void run() {
        try {
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
            weka.core.SerializationHelper.write(dir+"/"+name+".model", jrip);
        } catch (IOException ex) {
            Logger.getLogger(ExportRulesTask.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(ExportRulesTask.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
