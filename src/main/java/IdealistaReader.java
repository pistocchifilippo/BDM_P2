import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * To avoid hardcoded path will be used this class that contains utility methods that can read "idealista" folder.
 */
public class IdealistaReader {

    public static final String FILE_SEPARATOR = File.separator;
    private static final String IDEALISTA_FOLDER = "src_main_resources_idealista".replace("_",FILE_SEPARATOR);

    public static Stream<String> allDates() {
        return Arrays.stream(new File(IDEALISTA_FOLDER).list())
                .map(s -> s.split("_idealista")[0])
                .sorted();
    }

    public static Stream<String> allFilesName() {
        return allDates()
                .map(e -> filePerDate(e))
                .filter(e -> e.isPresent())
                .map(e -> e.get());
    }

    public static Optional<String> filePerDate(final String date) {
        return Arrays.stream(new File(IDEALISTA_FOLDER + FILE_SEPARATOR + date + "_idealista").list())
                .filter(s -> s.endsWith(".parquet"))
                .findFirst();
    }

    public static Stream<String> allFilesPath() {
        return allDates()
                .map(date -> new Tuple2<>(date, filePerDate(date)))
                .filter(t -> t._2.isPresent())
                .map(t -> IDEALISTA_FOLDER + FILE_SEPARATOR + t._1 + "_idealista" + FILE_SEPARATOR + t._2.get());
    }

    public static Stream<Tuple2<String,String>> allPairDateFilePath() {
        return allDates()
                .map(date -> new Tuple2<>(date, filePerDate(date)))
                .filter(t -> t._2.isPresent())
                .map(t -> new Tuple2<>(t._1, IDEALISTA_FOLDER + FILE_SEPARATOR + t._1 + "_idealista" + FILE_SEPARATOR + t._2.get()));
    }





}
