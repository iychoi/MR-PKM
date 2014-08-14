package edu.arizona.cs.mrpkm.commandline;

import edu.arizona.cs.mrpkm.augment.BloomMapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

/**
 *
 * @author iychoi
 */
public class MROutputFormatArgumentParser extends AArgumentParser {

    private final static String KEY_STRING = "f";
    private final static Class OUTPUT_FORMAT_CLASS_DEFAULT = MapFileOutputFormat.class;
    
    private Class value;
    
    public MROutputFormatArgumentParser() {
        this.value = OUTPUT_FORMAT_CLASS_DEFAULT;
    }
    
    @Override
    public String getKey() {
        return KEY_STRING;
    }

    @Override
    public Class getValueType() {
        return Class.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 1) {
            throw new ArgumentParseException("given args length is not 1");
        }
        
        if(args[0].equalsIgnoreCase(MapFileOutputFormat.class.getName())) {
            this.value = MapFileOutputFormat.class;
        } else if(args[0].equalsIgnoreCase(BloomMapFileOutputFormat.class.getName())) {
            this.value = BloomMapFileOutputFormat.class;
        } else if(args[0].equalsIgnoreCase("map") || args[0].equalsIgnoreCase("mapfile")) {
            this.value = MapFileOutputFormat.class;
        } else if(args[0].equalsIgnoreCase("bloom") || args[0].equalsIgnoreCase("bloommap") || args[0].equalsIgnoreCase("bloommapfile")) {
            this.value = BloomMapFileOutputFormat.class;
        } else {
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
    public Class getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : set output format";
    }
}
