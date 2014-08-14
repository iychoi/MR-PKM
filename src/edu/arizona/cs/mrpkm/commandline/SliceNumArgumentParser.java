package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class SliceNumArgumentParser extends AArgumentParser {

    private final static String KEY_STRING = "s";
    private final static int SLICE_NUM_DEFAULT = 1000;
    private final static int SLICE_NUM_PER_CORE = 3;
    
    private int coreSize = 0;
    private int value;
    
    public SliceNumArgumentParser() {
        this.value = SLICE_NUM_DEFAULT;
    }
    
    @Override
    public String getKey() {
        return KEY_STRING;
    }

    @Override
    public Class getValueType() {
        return Integer.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 1) {
            throw new ArgumentParseException("given args length is not 1");
        }
        
        try {
            this.value = Integer.parseInt(args[0]);
            if(this.value <= 0) {
                throw new ArgumentParseException("given slice num is too small");
            }
        } catch (NumberFormatException ex) {
            throw new ArgumentParseException("given arg is not in correct data type");
        }
    }

    @Override
    public boolean hasDefault() {
        return true;
    }
    
    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public int getValueLength() {
        return 1;
    }

    @Override
    public Integer getValue() {
        if(this.coreSize > 0) {
            return this.coreSize * SLICE_NUM_PER_CORE;
        } else {
            return this.value;
        }
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : set slice num";
    }

    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
    }
}
