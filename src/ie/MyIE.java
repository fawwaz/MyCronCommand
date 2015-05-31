package ie;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import cc.mallet.fst.CRF;
import cc.mallet.fst.MaxLatticeDefault;
import cc.mallet.types.FeatureVector;
import cc.mallet.types.InstanceList;
import cc.mallet.types.Sequence;

public class MyIE {
	public void doIE(){
		boolean isLocal = true;
		String lokasifile = "modelTABuatcobacoba";
		CRF crf;
		// Import ke database
		MyPipe pipe = new MyPipe(isLocal);
		pipe.setTargetProcessing(false);
		InstanceList testData = new InstanceList(pipe);
		testData.addThruPipe(new MyIterator(isLocal));
		
		
		ObjectInputStream s;
		try {
			s = new ObjectInputStream(new FileInputStream(lokasifile));
			crf = (CRF) s.readObject();
			s.close();
			
			// Buat koneksi ke database untuk siapin insert query
			for (int i = 0; i < testData.size(); i++) {
				Sequence input = (Sequence)testData.get(i).getData();
    	    	Sequence[] outputs = apply(crf, input, 1);// hanya 1 best option
    	    	int k = outputs.length;
    	    	boolean error = false;
    	    	
    	    	for (int a = 0; a < k; a++) {
    	    		if (outputs[a].size() != input.size()) {
    	    			System.out.println("Failed to decode input sequence " + i + ", answer " + a);
    	    			error = true;
    	    		}
    	    	}
    	    	
    	    	
    	    	if (!error) {
    	    		
    	    		for (int j = 0; j < input.size(); j++){
    	    			StringBuffer buf = new StringBuffer();

    	    			for (int a = 0; a < k; a++){
    	    				buf.append(outputs[a].get(j).toString()).append(" ");
							//System.out.println(outputs[a].get(j).toString());
    	    			}
    	    			
    	    			FeatureVector fv = (FeatureVector)input.get(j);
	    				buf.append(fv.toString(true));

    	    			String twitter_tweet_id = (String) testData.get(i).getName();
    	    			//Harus Diganti jumlahnya ini sejenis offset
    	    			// [LabelAnotasiFinal] OFFSET
    	    			/*
    	    			 fold 1 : 1420
    	    			 fold 2 : 2746
    	    			 fold 3 : 3974
    	    			 fold 4 : 5248
    	    			 fold 5 : 6541
    	    			 fold 6 : 7893
    	    			 fold 7 : 9326
    	    			 fold 8 : 10631
    	    			 fold 9 : 11906 / fold 8 belum dilakukan 
    	    			 fold 10  : 13242
    	    			*/
    	    			int offset = 0;
    	    			//System.out.println("Offset saat ini adalah "+offset);
						//Long curr_sequence_id = base + j+offset ;//+ 11906 ;//+10500;
						//updater.UpdateLabelAnotasiTweetFinal(buf.toString(), curr_sequence_id);
						System.out.println(">"+buf.toString()+ " Name (Start): "+twitter_tweet_id);
					}

					System.out.println();
				}
			}
			
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	
		
		
		
	}

	private Sequence[] apply(CRF model, Sequence input, int k) {
		Sequence[] answers;
		if (k == 1) {
			answers = new Sequence[1];
			answers[0] = model.transduce (input);
		}
		else {
			MaxLatticeDefault lattice =	new MaxLatticeDefault (model, input, null, 100000); // default cache is 100000

			answers = lattice.bestOutputSequences(k).toArray(new Sequence[0]);
		}
		return answers;
	}
}
