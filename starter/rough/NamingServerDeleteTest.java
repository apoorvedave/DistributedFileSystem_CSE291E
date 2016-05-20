package rough;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by apoorve on 15/05/16.
 */
public class NamingServerDeleteTest {
    public static void main(String[] args) {
        Map<String, Integer> map = new HashMap<>();
        map.put("hello", 1);
        map.put("Hi", 2);
        map.put("hell", 6);
        System.out.println(map);
        Set<String> keys = map.keySet();
        Iterator<String> it = keys.iterator();
        while (it.hasNext()){
            String key = it.next();
            if (key.startsWith("hel")) {
                it.remove();
            }
        }
        System.out.println(map);
    }
}
