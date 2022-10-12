package org.apache.flink.playgrounds.ops.clickcount.util;


import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class DimUtil {

    private static Map<String,String> cache = new HashMap<>();

    static{
        cache.put("/jobs","1");
        cache.put("/about","2");
    }

    public static Tuple2<String,Boolean> query(String key){
        String value = cache.get(key);
        if(value!=null){
            return new Tuple2(value,true);
        }else{
            value = key.hashCode()%5+"";
            return new Tuple2(value,false);
        }
    }

}
