/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uff.ic.swlab.tranning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import static uff.ic.swlab.tranning.JRIP_Tranning.TF;
import static uff.ic.swlab.tranning.JRIP_Tranning.idf;

/**
 *
 * @author angelo
 */
public class TaskBuildVector implements Callable<Float[]> {

    private int size;
    private String dataset;
    private ArrayList<String> category_dataset;
    private ArrayList<String> datasets;
    Map<String, Integer> indices_categories;

    public TaskBuildVector(int size, String dataset, ArrayList<String> category_dataset, ArrayList<String> datasets, Map<String, Integer> indices_categories) {
        this.size = size;
        this.dataset = dataset;
        this.category_dataset = category_dataset;
        this.datasets = datasets;
        this.indices_categories = indices_categories;
    }

    @Override
    public Float[] call() throws Exception {
        Float[] vetor = new Float[size];
        Arrays.fill(vetor, new Float(0));
        for (String category : category_dataset) {
            float value_tf = TF(category, dataset);
            float value_idf = idf(category, datasets);
            float value_tf_idf = value_tf * value_idf;
            int index = indices_categories.get(category);
            vetor[index] = value_tf_idf;
        }
        return vetor;

    }

}
