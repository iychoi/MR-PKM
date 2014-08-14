package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class MultiPathArgumentParser extends AEmptyKeyArgumentParser {

    private int minNum = 0;
    private int maxNum = 0; // unlimited
    private String[] value;
    
    public MultiPathArgumentParser() {
        this.minNum = 0;
        this.maxNum = 0; // unlimited
        this.value = null;
    }
    
    public MultiPathArgumentParser(int min) {
        this.minNum = min;
        this.maxNum = 0; // unlimited
        this.value = null;
    }
    
    public MultiPathArgumentParser(int min, int max) {
        this.minNum = min;
        this.maxNum = max;
        this.value = null;
    }
    
    @Override
    public Class getValueType() {
        return String[].class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length < this.minNum) {
            throw new ArgumentParseException("given args length is less than min " + this.minNum);
        }
        
        if(this.maxNum != 0 && args.length > this.maxNum) {
            throw new ArgumentParseException("given args length is greater than max " + this.maxNum);
        }
        
        this.value = args;
    }
    
    @Override
    public int getValueLength() {
        return -1;
    }

    @Override
    public String[] getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return "<Path...>";
    }
    
    @Override
    public boolean isMandatory() {
        return true;
    }
}
