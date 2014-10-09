package edu.arizona.cs.mrpkm.types;

/**
 *
 * @author iychoi
 */
public class MutableInteger {
    private int value;
    
    public MutableInteger(int value) {
        this.value = value;
    }
    
    public void set(int n) {
        this.value = n;
    }
    
    public int get() {
        return this.value;
    }
    
    public void increase() {
        this.value++;
    }
}
