package edu.rmit.cosc2367.s3806186.Assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable, WritableComparable<WordPair>{
	// State variables
	private Text word; // Save word
	private Text neighbour; // Save neighbour
	
	// Constructors
	public WordPair() {
		this.word = new Text(); 
		this.neighbour = new Text(); 
	}
	
	// Inherited methods implementation
	@Override
	public int compareTo(WordPair o) {
		// Check if words compared are different
		int compareRes = this.word.compareTo(o.getWord());
		if(compareRes != 0) {
			// If they are different return result
			return compareRes;
		}
		
		// If they are same, we need to check if neighbour is * for 
		// either of the words being compared
		if(this.neighbour.toString().equals("*")) {
			return -1; // Current word neighbour is *
		} else if(o.getNeighbour().toString().equals("*")) {
			return 1; // Compared word neighbour is *
		}
		// If neither neighbour * then return natual comparison result
		return this.neighbour.compareTo(o.getNeighbour());
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		word.readFields(arg0);
        neighbour.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		word.write(arg0);
        neighbour.write(arg0);
	}

	
	// Other methods
	@Override
    public String toString() {
        return "("+word+", "+neighbour+")";
    }
	
	// Getter and Setters
    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbour(String neighbor){
        this.neighbour.set(neighbor);
    }

    public Text getWord() {
        return word;
    }

    public Text getNeighbour() {
        return neighbour;
    }
}
