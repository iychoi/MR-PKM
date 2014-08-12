package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class IndexSearchPathArgumentParser extends ArgumentParserBase {

    private final static String KEY_STRING = "i";
    
    private String value;
    
    public IndexSearchPathArgumentParser() {
        this.value = null;
    }
    
    @Override
    public String getKey() {
        return KEY_STRING;
    }

    @Override
    public Class getValueType() {
        return String.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 1) {
            throw new ArgumentParseException("given args length is not 1");
        }
        
        this.value = args[0];
    }

    @Override
    public boolean hasDefault() {
        return false;
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
    public String getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : set index search path";
    }
}
