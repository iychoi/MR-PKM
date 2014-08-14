package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class MatchHitMinFilterArgumentParser extends AArgumentParser {

    private final static String KEY_STRING = "min";
    private final static int MIN_DEFAULT = 0;
    
    private int value;
    
    public MatchHitMinFilterArgumentParser() {
        this.value = MIN_DEFAULT;
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
            if(this.value < 0) {
                throw new ArgumentParseException("given number is too small");
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
        return false;
    }

    @Override
    public int getValueLength() {
        return 1;
    }

    @Override
    public Integer getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : set match hit filter min";
    }
}
