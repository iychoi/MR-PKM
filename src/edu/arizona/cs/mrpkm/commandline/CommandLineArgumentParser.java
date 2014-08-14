package edu.arizona.cs.mrpkm.commandline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class CommandLineArgumentParser {
    private Hashtable<String, AArgumentParser> argumentParserTable = new Hashtable<String, AArgumentParser>();
    private List<AArgumentParser> argumentParserList = new ArrayList<AArgumentParser>();
    public final static String OPTION_PREFIX = "--";
    private final static String NULL_OPTION_KEY = "null";
    
    public CommandLineArgumentParser() {
    
    }
    
    public void addArgumentParser(AArgumentParser arg) throws Exception {
        String key = arg.getKey();
        if(key == null) {
            key = NULL_OPTION_KEY;
        }
        
        AArgumentParser existing = this.argumentParserTable.get(key);
        if(existing != null) {
            throw new Exception("the same argument key (" + key + ") is already defined");
        }

        this.argumentParserTable.put(key, arg);
        this.argumentParserList.add(arg);
    }
    
    private String getKeyFromOptionString(String option) throws ArgumentParseException {
        if(!option.startsWith(OPTION_PREFIX)) {
            throw new ArgumentParseException("option string does not starts with " + OPTION_PREFIX);
        }
        
        return option.substring(OPTION_PREFIX.length());
    }
    
    public AArgumentParser[] parse(String[] args) throws ArgumentParseException {
        boolean[] argsProcessed = new boolean[args.length];
        for(int i=0;i<argsProcessed.length;i++) {
            argsProcessed[i] = false;
        }
        
        Hashtable<String, AArgumentParser> processed = new Hashtable<String, AArgumentParser>();
        
        // parse -- key
        List<AArgumentParser> result = new ArrayList<AArgumentParser>();
        for(int i=0;i<args.length;i++) {
            if(args[i].startsWith(OPTION_PREFIX)) {
                String key = getKeyFromOptionString(args[i]);
                AArgumentParser argParser = this.argumentParserTable.get(key);
                if(argParser == null) {
                    throw new ArgumentParseException("a parser for an argument " + args[i] + " is not registered");
                }
                
                int valLen = argParser.getValueLength();
                if(i + valLen >= args.length) {
                    throw new ArgumentParseException("arguments for " + args[i] + " option is not properly given");
                }
                
                String[] argVals = new String[valLen];
                System.arraycopy(args, i + 1, argVals, 0, valLen);
                argParser.parse(argVals);
                processed.put(argParser.getKey(), argParser);
                argsProcessed[i] = true;
                for(int j=0;j<valLen;j++) {
                    argsProcessed[i+1+j] = true;
                }
            }
        }
        
        // check default
        Collection<AArgumentParser> values = this.argumentParserTable.values();
        for(AArgumentParser argParser : values) {
            if(argParser.getKey() != null) {
                if(processed.get(argParser.getKey()) == null) {
                    if(argParser.hasDefault()) {
                        processed.put(argParser.getKey(), argParser);
                    }
                }
            }
        }
        
        // check empty key
        List<String> remains = new ArrayList<String>();
        for(int i=0;i<argsProcessed.length;i++) {
            if(!argsProcessed[i]) {
                remains.add(args[i]);
                argsProcessed[i] = true;
            }
        }
        
        if(remains.size() > 0) {
            AArgumentParser emptyKeyParser = this.argumentParserTable.get(NULL_OPTION_KEY);
            if(emptyKeyParser == null) {
                throw new ArgumentParseException("empty key parser is not registered");
            }
            
            emptyKeyParser.parse(remains.toArray(new String[0]));
            processed.put(NULL_OPTION_KEY, emptyKeyParser);
        }
        
        // check mandatory arguments
        Collection<AArgumentParser> mandatories = this.argumentParserTable.values();
        for(AArgumentParser argParser : mandatories) {
            if(argParser.isMandatory()) {
                // check it exists in result
                if(argParser.getKey() == null) {
                    if(processed.get(NULL_OPTION_KEY) == null) {
                        throw new ArgumentParseException("mandatory argument is not given");
                    }
                } else {
                    if(processed.get(argParser.getKey()) == null) {
                        throw new ArgumentParseException("mandatory option " + OPTION_PREFIX + argParser.getKey() + " is not given");
                    }
                }
            }
        }
        
        // sort results to registered order
        for(int i=0;i<this.argumentParserList.size();i++) {
            String key = this.argumentParserList.get(i).getKey();
            if(key == null) {
                key = NULL_OPTION_KEY;
            }
            AArgumentParser apb = processed.get(key);
            if(apb != null) {
                result.add(apb);
            }
        }
        
        return result.toArray(new AArgumentParser[0]);
    }

    public String getHelpMessage() {
        StringBuilder sb = new StringBuilder();
        AArgumentParser nullParser = this.argumentParserTable.get(NULL_OPTION_KEY);
        sb.append("Usage : <options> ");
        if(nullParser != null) {
            sb.append(nullParser.getHelpMessage());
        }
        sb.append("\n");
        sb.append("\n");
        
        sb.append("Options\n");
        Collection<AArgumentParser> values = this.argumentParserTable.values();
        for(AArgumentParser argParser : values) {
            if(argParser.getKey() != null) {
                sb.append(argParser.getHelpMessage());
                sb.append("\n");
            }
        }
        
        return sb.toString();
    }
}
