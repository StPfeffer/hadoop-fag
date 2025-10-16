import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetflixAnalyzer {

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "de", "em", "para", "a", "o", "e", "do", "da", "no", "na", "um", "uma", "com", 
        "que", "se", "os", "as", "por", "and", "the", "of", "to", "in", "is", "on",
        "s", "t", "re", "ve", "ll", "d", "m", "i", "you", "he", "she", "it", "we", "they",
        "his", "her", "him", "my", "your", "our", "at", "from", "by", "an", "this",
        "that", "as", "but", "or", "if", "of", "for"
    ));

    public static class NetflixMapper extends Mapper<LongWritable, Text, Text, Text> {

        // Trata CSV complexo com aspas e vírgulas internas
        private Pattern csvPattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("show_id")) {
                return;
            }

            String[] fields = csvPattern.split(line);
            if (fields.length < 12) {
                System.err.println("Linha inválida (poucos campos): " + line);
                return;
            }

            String title = safeClean(fields[2]);
            String description = safeClean(fields[11]);

            String normalized = description.toLowerCase()
                .replaceAll("[^a-záéíóúãõç'\\s]", " ")
                .replaceAll("\\s+", " ")
                .trim();

            if (normalized.isEmpty()) {
                return;
            }

            // Extrai palavras incluindo contrações
            Pattern wordPattern = Pattern.compile("\\b[a-záéíóúãõç]+(?:'[a-záéíóúãõç]+)?\\b");
            Matcher matcher = wordPattern.matcher(normalized);
            List<String> words = new ArrayList<>();
            while (matcher.find()) {
                String word = matcher.group();
                // Remove possessivos
                if (word.endsWith("'s")) {
                    word = word.substring(0, word.length() - 2);
                }
                if (!word.isEmpty()) {
                    words.add(word);
                }
            }

            int wordCount = words.size();
            if (wordCount > 0) {
                context.write(new Text("count_info"), new Text(wordCount + ":" + title));
                context.write(new Text("avg_words"), new Text(wordCount + ":1"));
            }

            int nonStopCount = 0;
            for (String word : words) {
                if (STOPWORDS.contains(word)) continue;

                // Expandir contrações comuns
                String cleanedWord = expandContractions(word);
                if (cleanedWord.isEmpty() || STOPWORDS.contains(cleanedWord)) continue;

                context.write(new Text("word_freq"), new Text(cleanedWord + ":1"));
                nonStopCount++;
            }

            if (nonStopCount > 0) {
                context.write(new Text("total_words"), new Text(String.valueOf(nonStopCount)));
            }
        }

        private String expandContractions(String word) {
            switch (word) {
                case "that's":
                    return "that";
                case "it's":
                    return "it";
                case "don't":
                    return "do not"; // ou apenas "dont" se preferir contar como uma
                case "didn't":
                    return "did not";
                case "won't":
                    return "will not";
                case "can't":
                    return "cannot";
                case "i'm":
                    return "i";
                case "you're":
                    return "you";
                case "he's":
                    return "he";
                case "she's":
                    return "she";
                case "we're":
                    return "we";
                case "they're":
                    return "they";
                default:
                    return word.replaceAll("'s$", ""); // remove possessivo
            }
        }
        
        private String safeClean(String field) {
            if (field == null) return "";
            return field.trim().replaceAll("^\"|\"$", "");
        }

    }

    public static class NetflixReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> wordFreq = new HashMap<>();

        private long totalWords = 0;

        private int descCount = 0;

        private long sumWords = 0;

        private TreeMap<Integer, String> lengthMap = new TreeMap<>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();

            if (keyStr.equals("word_freq")) {
                for (Text val : values) {
                    String[] parts = val.toString().split(":");
                    if (parts.length != 2) {
                        continue;
                    }

                    String word = parts[0];
                    wordFreq.put(word, wordFreq.getOrDefault(word, 0) + 1);
                }
            }
            else if (keyStr.equals("count_info")) {
                for (Text val : values) {
                    String[] parts = val.toString().split(":", 2);
                    if (parts.length != 2) {
                        continue;
                    }

                    try {
                        int count = Integer.parseInt(parts[0].trim());
                        String title = parts[1].trim();
 
                        lengthMap.put(count, title);
                        sumWords += count;
                        descCount++;
                    } catch (NumberFormatException e) {
                        System.err.println("Erro parsing count_info: " + val);
                        continue;
                    }
                }
            } 
            else if (keyStr.equals("total_words")) {
                for (Text val : values) {
                    totalWords += Long.parseLong(val.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (descCount == 0) {
                return;
            }

            // RESULTADOS ORDENADOS
            String maxTitle = lengthMap.lastEntry().getValue();
            String minTitle = lengthMap.firstEntry().getValue();

            context.write(new Text("Título com descrição mais longa"), 
                new Text(maxTitle + " (" + lengthMap.lastKey() + " palavras)"));
            context.write(new Text("Título com descrição mais curta"), 
                new Text(minTitle + " (" + lengthMap.firstKey() + " palavras)"));

            context.write(new Text("Total de palavras em todas as descrições"), 
                new Text(String.valueOf(totalWords)));

            double avg = (double) sumWords / descCount;
            context.write(new Text("Média de palavras por descrição"), 
                new Text(String.format("%.2f", avg)));

            // TOP 5 MAIS FREQUENTES
            List<Map.Entry<String, Integer>> sorted = new ArrayList<>(wordFreq.entrySet());
            sorted.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            StringBuilder top5more = new StringBuilder();
            for (int i = 0; i < Math.min(5, sorted.size()); i++) {
                top5more.append(sorted.get(i).getKey())
                    .append(":").append(sorted.get(i).getValue()).append(" ");
            }
            context.write(new Text("Top 5 palavras mais repetidas"), 
                new Text(top5more.toString().trim()));

            // TOP 5 MENOS FREQUENTES
            sorted.sort(Map.Entry.comparingByValue());
            StringBuilder top5less = new StringBuilder();
            for (int i = 0; i < Math.min(5, sorted.size()); i++) {
                top5less.append(sorted.get(i).getKey())
                    .append(":").append(sorted.get(i).getValue()).append(" ");
            }
            context.write(new Text("Top 5 palavras menos repetidas"), 
                new Text(top5less.toString().trim()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: NetflixAnalyzer <input> <output>");
            System.exit(2);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(conf, "Netflix Analyzer");
        job.setJarByClass(NetflixAnalyzer.class);
        job.setMapperClass(NetflixMapper.class);
        job.setReducerClass(NetflixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(inputPath)) {
            System.err.println("ERRO: Input não existe: " + args[0]);
            System.exit(1);
        }

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

