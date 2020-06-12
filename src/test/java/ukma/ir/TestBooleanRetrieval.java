package ukma.ir;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ukma.ir.index.IndexService;

import java.nio.file.Paths;
import java.util.List;


public class TestBooleanRetrieval {
    private static QueryProcessor queryProcessor;

    @BeforeAll
    static void initIndex() {
        IndexService.clearCache();
        queryProcessor = QueryProcessor.initQueryProcessor
                (IndexService.buildIndex(
                        Paths.get("G:\\project\\infoSearch\\src\\test\\java\\ukma\\ir\\testing_files")));
    }

    @Test
    void testSimpleSuccessfulSearch() {
        List<String> res = queryProcessor.processBooleanQuery("fell AND already");
        Assertions.assertTrue(res.size() == 1 && res.get(0).equals("test4.txt"));
    }

    @Test
    void testCompoundSuccessfulSearch() {
        List<String> res = queryProcessor.processBooleanQuery("fell AND already OR engine AND NOT trigger OR pen AND PiNeApPlE");
        Assertions.assertTrue(res.size() == 3
                && res.contains("test1.txt")
                && res.contains("test4.txt")
                && res.contains("test5.txt"));
    }

    @Test
    void testUnsuccessfulSearch() {
        List<String> res = queryProcessor.processBooleanQuery("fell AND already AND off");
        Assertions.assertEquals(0, res.size());
    }

    @Test
    void testWrongInputFormat() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            queryProcessor.processBooleanQuery("fell $! off");
        });
    }

    @AfterAll
    static void clearIndex() {
        IndexService.clearCache();
    }
}
