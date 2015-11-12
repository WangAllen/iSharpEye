package org.wyh.mahout.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class TestMahout {

	public static void test_mahout() {
		try {
			DataModel model = new FileDataModel(new File(""));
			UserSimilarity sim = new PearsonCorrelationSimilarity(model);
			UserNeighborhood nbh = new NearestNUserNeighborhood(10, sim, model);
			Recommender rec = new GenericUserBasedRecommender(model, nbh, sim);
			
			List<RecommendedItem> recItemList = rec.recommend(0, 0);
			for (RecommendedItem item : recItemList) {
				System.out.println(item);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TasteException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
	}
}
