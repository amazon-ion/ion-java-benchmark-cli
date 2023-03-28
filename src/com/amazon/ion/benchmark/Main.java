package com.amazon.ion.benchmark;

import org.docopt.Docopt;

import java.util.Map;

public class Main {

    private static final String TITLE = "IonJava Benchmarking Tool\n\n";

    private static final String DESCRIPTION =
        "Description:\n\n"

        + "  Tool that allows users to...\n"
        + "    * Determine which IonJava configurations perform best\n"
        + "    * Compare IonJava to Java implementations of other serialization formats\n"
        + "  ...for the individual users' data and access patterns.\n\n"

        + "  Additionally, allows IonJava developers to...\n"
        + "    * Determine the impact of a proposed change\n"
        + "    * Decide where investments should be made in improving performance\n"
        + "  ...by generating results from a variety of real-world data and access patterns.\n"
        + "\n\n";

    private static final String USAGE =
        "Usage:\n"

        + "  ion-java-benchmark write [--profile] [--limit <int>] [--mode <mode>] [--time-unit <unit>] "
            + "[--warmups <int>] [--iterations <int>] [--forks <int>] [--results-format <type>] "
            + "[--results-file <file>] [--io-type <type>]... [--io-buffer-size <int>]... [--format <type>]... "
            + "[--api <api>]... [--ion-imports-for-input <file>] [--ion-imports-for-benchmark <file>]... "
            + "[--ion-flush-period <int>]... [--ion-length-preallocation <int>]... [--ion-float-width <int>]... "
            + "[--ion-use-symbol-tokens <bool>]... [--ion-writer-block-size <int>]... "
            + "[--json-use-big-decimals <bool>]... <input_file>\n"

        + "  ion-java-benchmark read [--profile] [--limit <int>] [--mode <mode>] [--time-unit <unit>] "
            + "[--warmups <int>] [--iterations <int>] [--forks <int>] [--results-format <type>] "
            + "[--results-file <file>] [--io-type <type>]... [--io-buffer-size <int>]... [--format <type>]... "
            + "[--api <api>]... [--ion-imports-for-input <file>] [--ion-imports-for-benchmark <file>]... "
            + "[--ion-flush-period <int>]... [--ion-length-preallocation <int>]... [--ion-float-width <int>]... "
            + "[--ion-use-symbol-tokens <bool>]... [--paths <file>] [--ion-reader <type>]... "
            + "[--ion-use-lob-chunks <bool>]... [--ion-use-big-decimals <bool>]... [--ion-reader-buffer-size <int>]... "
            + "[--json-use-big-decimals <bool>]... <input_file>\n"

        + "  ion-java-benchmark run-suite (--test-ion-data <file_path>) (--benchmark-options-combinations <file_path>) <output_file>\n"

        + "  ion-java-benchmark --help\n"

        + "  ion-java-benchmark --version\n\n";

    private static final String COMMANDS =
        "Commands:\n"

        + "  write    Benchmark writing the given input file to the given output format(s). In order to isolate "
            + "writing from reading, during the setup phase write instructions are generated from the input file "
            + "and stored in memory. For large inputs, this can consume a lot of resources and take a long time "
            + "to execute. This may be reduced by using the --limit option to limit the number of entries that "
            + "are written. The cost of initializing the writer is included in each timed benchmark invocation. "
            + "Therefore, it is important to provide data that closely matches the size of the data written by a "
            + "single writer instance in the real world to ensure the initialization cost is properly amortized.\n"

        + "  read     First, re-write the given input file to the given output format(s) (if necessary), then "
            + "benchmark reading the resulting log files. If this takes too long to complete, consider using "
            + "the --limit option to limit the number of entries that are read. Specifying non-default settings "
            + "for certain options will cause the input data to be re-encoded even if the requested format is the "
            + "same as the format of the provided input. These options are --ion-length-preallocation and "
            + "--ion-flush-period for input in the ion binary format. The cost of initializing the reader or "
            + "DOM loader is included in each timed benchmark invocation. Therefore, it is important to provide "
            + "data that closely matches the size of data read by a single reader/loader instance in the real "
            + "world to ensure the initialization cost is properly amortized.\n"

        + "\n";

    private static final String OPTIONS =
        "Options:\n"

        // Option commands:

        + "  -h --help                              Show this screen.\n"

        + "  -v --version                           Show the version of this tool and ion-java.\n"

        // Common options:

        + "  -p --profile                           Initiates a single iteration that repeats indefinitely until "
            + "terminated, allowing users to attach profiling tools. If this option is specified, the --warmups, "
            + "--iterations, and --forks options are ignored. An error will be raised if this option is used when "
            + "multiple values are specified for other options. Not enabled by default.\n"

        + "  -n --limit <int>                       Maximum number of entries to process. By default, all entries in "
            + "each input file are processed.\n"

        + "  -m --mode <mode>                       The JMH benchmark mode to use, from the set (SingleShotTime | "
            + "SampleTime | AverageTime | Throughput). SingleShotTime, in which each benchmark iteration writes "
            + "or reads the data exactly once, is usually sufficient for medium and large streams. SampleTime, "
            + "AverageTime, and Throughput all attempt to perform multiple reads or writes per iteration, which "
            + "works best for smaller streams. If variance is high between iterations when using SingleShotTime, "
            + "consider trying a different mode. [default: SingleShotTime]\n"

        + "  -u --time-unit <unit>                  The TimeUnit in which benchmark results will be reported, from the "
            + "set (microseconds | milliseconds | seconds). For small streams a more precise unit may be "
            + "necessary, as JMH rounds to three digits after the decimal point. [default: milliseconds]\n"

        + "  -w --warmups <int>                     Number of benchmark warm-up iterations. [default: 10]\n"

        + "  -i --iterations <int>                  Number of benchmark iterations. [default: 10]\n"

        + "  -F --forks <int>                       Number of benchmark forks (distinct JVMs). [default: 1]\n"

        + "  -r --results-format <type>             Format for the benchmark results, from the set (jmh | ion). "
            + "Specifying an option other than jmh will cause the results to be written to a file. "
            + "[default: jmh]\n"

        + "  -o --results-file <path>               Destination for the benchmark results. By default, results will be "
            + "written to stdout unless a results format other than jmh is specified, in which case the results "
            + "will be written to a file with the default name 'jmh-result'.\n"

        + "  -t --io-type <type>                    The source or destination type, from the set (buffer | file). If "
            + "buffer is selected, buffers the input data in memory before reading and writes the output data to "
            + "an in-memory buffer instead of a file. To limit the amount of memory required, use --limit. May be "
            + "specified multiple times to compare both settings."
            + "[default: file]\n"

        + "  -z --io-buffer-size <int>              The size in bytes of the internal buffer of the "
            + "BufferedInputStream that wraps the input file (for read benchmarks) or BufferedOutputStream / "
            + "ByteArrayOutputStream that wraps the output file or buffer (for write benchmarks), or 'auto', which "
            + "uses the stream's default buffer size. Ignored for read benchmarks when --io-type buffer is used "
            + "because this mode reads the entire input directly from a properly-sized buffer. May be specified "
            + "multiple times to compare different settings. [default: auto]\n"

        + "  -f --format <type>                     Format to benchmark, from the set (ion_binary | ion_text | json | "
            + "cbor). May be specified multiple times to compare different formats. [default: ion_binary]\n"

        + "  -a --api <api>                         The API to exercise (dom or streaming). For the ion-binary or "
            + "ion-text formats, 'streaming' causes IonReader/IonWriter to be used while 'dom' causes IonLoader to be "
            + "used. For Jackson JSON, 'streaming' causes JsonParser/JsonGenerator to be used while 'dom' causes "
            + "ObjectMapper to materialize JsonNode instances. May be specified multiple times to compare both "
            + "APIs. [default: streaming]\n"

        + "  -I --ion-imports-for-input <file>      A file containing a sequence of Ion symbol tables, or the string "
            + "'none'. Any Ion data read from <input_file> will use these as a catalog from which to resolve "
            + "shared symbol table imports. This occurs when preparing read and write benchmarks. Ignored unless "
            + "one of the specified formats is ion_binary or ion_text. This option must be provided when "
            + "<input_file> is Ion data that contains shared symbol table imports. [default: none]\n"

        + "  -c --ion-imports-for-benchmark <file>  A file containing a sequence of Ion symbol tables, or the string "
            + "'none' or 'auto'. Any Ion data written for benchmarks will declare these symbol tables as shared "
            + "symbol table imports and resolve symbols against them when writing the stream. For write "
            + "benchmarks, <input_file> data in the Ion format is read using the shared symbol tables declared by "
            + "--ion-imports-for-input; then, during the timed portion of the benchmark, the streams are written "
            + "using these imports. For read benchmarks, <input_file> data in the Ion format is re-encoded using "
            + "the shared symbol tables declared by --ion-imports-for-input for reading and "
            + "--ion-imports-for-output for writing; then, during the timed portion of the benchmark, the streams "
            + "are read using these imports. Ignored unless one of the specified formats is ion_binary or "
            + "ion_text. May be specified multiple times to compare the use of different imports. 'none' causes "
            + "Ion data to be written without shared symbol tables. 'auto' causes the setting provided to "
            + "--ion-imports-for-input to be used here. In other words, using 'auto', Ion data will always be "
            + "written using the same shared symbol tables with which it was read, if any. [default: auto]\n"

        + "  -d --ion-flush-period <int>            The number of top-level values to write between flushes, or "
            + "'auto'. Each flush initiates a new local symbol table append. Ignored unless one of the specified "
            + "formats is ion-binary. May be specified multiple times to compare multiple settings. The 'auto' setting "
            + "behaves as follows: for read benchmarks, if the other settings make it possible to use the input "
            + "data as-is, the input will not be re-encoded. If other settings require the input to be re-encoded, "
            + "then flushes will occur at every symbol table encountered in the input, preserving the existing "
            + "symbol table boundaries. For write benchmarks, no incremental flushes will be performed; the entire "
            + "output will be buffered until writing completes. [default: auto]\n"

        + "  -L --ion-length-preallocation <int>    Number of bytes that the Ion writer will preallocate for length "
            + "fields, from the set (0, 1, 2, auto). Lower numbers lead to a more compact encoding; higher numbers "
            + "lead to faster writing. May be specified multiple times to compare different settings. The 'auto' "
            + "setting behaves is as follows: for read benchmarks, if the other settings make it possible to use "
            + "the input data as-is, the input will not be re-encoded. If other settings require the input to be "
            + "re-encoded, then 2-byte preallocation will be used. For write benchmarks, 'auto' causes 2-byte "
            + "preallocation to be used. Ignored unless --format is ion_binary. [default: auto]\n"

        + "  -W --ion-float-width <int>             The bit width of binary Ion float values (32 | 64 | auto). "
            + "Ignored unless --format is ion_binary. May be specified multiple times to compare different "
            + "settings. 'auto' behaves as follows: for write benchmarks, 64-bit floats are written; for read "
            + "benchmarks, the input file is read as-is unless the values of other options require it to be "
            + "re-written, in which case it will be re-written and later read using 64-bit floats. "
            + "[default: auto]\n"

        + "  -k --ion-use-symbol-tokens <bool>      When reading and/or writing Ion field names, annotations, and "
            + "symbol values, determines whether to use Ion APIs that return and accept SymbolToken objects rather "
            + "than Strings. Either 'true' or 'false'. Ignored unless --format is ion_text or ion_binary and --ion-api "
            + "is streaming. Must be 'true' when the streaming APIs are used with Ion streams that contain symbols "
            + "with unknown text. May be specified twice to compare both settings. [default: false]\n"

        + "  -g --json-use-big-decimals <bool>      When reading and/or writing JSON non-integer numeric values, use "
            + "BigDecimal in order to preserve precision. When false, `double` will be used and precision may be lost. "
            + "May be specified twice to compare both settings. [default: true]\n"

        // 'write' options:

        + "  -b --ion-writer-block-size <int>       The size in bytes of the blocks the binary IonWriter uses to "
            + "buffer data, or 'auto', which uses the default value provided by the Ion writer builder. May be "
            + "specified multiple times to compare different values. Ignored unless the format is ion_binary. "
            + "[default: auto]\n"

        // 'read' options:

        + "  -s --paths <file>                      A file containing a sequence of Ion s-expressions representing "
            + "search paths (https://github.com/amzn/ion-java-path-extraction/#search-paths) into the input data. "
            + "Only values matching one of the paths will be materialized; all other values will be skipped. For "
            + "Ion data, registering at least one search path causes the ion-java-path-extraction "
            + "(https://github.com/amzn/ion-java-path-extraction/) extension to ion-java to be used. For other "
            + "formats, the most efficient known API for performing sparse reads will be used. Ignored unless "
            + "--ion-api streaming is specified. By default, no search paths will be used, meaning the input data "
            + "will be fully traversed and materialized.)\n"

        + "  -R --ion-reader <type>                 The IonReader type to use, from the set (incremental | "
            + "non_incremental). May be specified multiple times to compare different readers. Note: because the DOM "
            + "uses IonReader to parse data, this option is applicable for read benchmarks with both options for "
            + "--ion-api. Ignored unless --format is ion_binary. [default: incremental]\n"

        + "  -e --ion-use-lob-chunks <bool>         When true, read Ion blobs and clobs in chunks into a reusable "
            + "buffer of size 1024 bytes using `IonReader.getBytes`. When false, use `IonReader.newBytes`, which "
            + "allocates a properly-sized buffer on each invocation. Ignored unless --format is ion_binary or ion_text "
            + "and --ion-api streaming is used. May be specified twice to compare both settings. [default: false]\n"

        + "  -D --ion-use-big-decimals <bool>       When true, read Ion decimal values into BigDecimal instances. When "
            + "false, read decimal values into Decimal instances, which are capable of conveying negative zero. "
            + "Ignored unless --format is ion_binary or ion_text and --ion-api streaming is used. May be specified "
            + "twice to compare both settings. [default: false]\n"

        + "  -Z --ion-reader-buffer-size <int>      The initial size of the incremental reader's buffer, or 'auto', "
            + "which uses either the incremental reader's default initial buffer size value or the total length of the "
            + "stream (whichever is smaller). To avoid resizing, this value should be larger than the largest "
            + "top-level value in the Ion stream. Ignored unless --format ion_binary and --ion-reader incremental are "
            + "specified. May be specified multiple times to compare different settings.\n"

        // 'run-suite' options

        + "  -G --test-ion-data <file_path>      This option will specify the path of the directory which contains all test Ion data.\n"

        + "  -B --benchmark-options-combinations <file_path>      This option will specify the path of an Ion text file which contains all options combinations of ion-java-benchmark-cli."

        + "\n";

    private static final String EXAMPLES =
        "Examples:\n\n"

        + "  Benchmark a full-traversal read of example.10n from file using the IonReader API, with 10 warmups, 10 "
            + "iterations, and 1 fork, printing the results to stdout in JMH’s standard text format.\n\n"

        + "  ion-java-benchmark read example.10n\n\n"

        + "  Benchmark a fully-buffered write of binary Ion data equivalent to example.10n to file using the "
            + "IonWriter API, with 10 warmups, 10 iterations, and 1 fork, printing the results to stdout in JMH’s "
            + "standard text format.\n\n"

        + "  ion-java-benchmark write example.10n\n\n"

        + "  Benchmark a write of binary Ion data equivalent to the first 1,000 top-level values in example.10n to "
            + "in-memory bytes using the IonWriter API, flushing after every 100 top-level values. Produce "
            + "results for both 0-byte length preallocation and 2-byte length preallocation to facilitate "
            + "comparison of both settings.\n\n"

        + "  ion-java-benchmark write --io-type buffer \\\n"
        + "                           --limit 1000 \\\n"
        + "                           --ion-flush-period 100 \\\n"
        + "                           --ion-length-preallocation 0 \\\n"
        + "                           --ion-length-preallocation 2 \\\n"
        + "                           example.10n\n\n"

        + "  Profile a sparse read of example.10n from file, materializing only the values that match the paths "
            + "specified in paths.ion,  using ion-java-path-extraction. This process will repetitively execute "
            + "until manually terminated, allowing the user to attach a tool for gathering performance profiles.\n\n"

        + "  ion-java-benchmark read --profile --paths paths.ion example.10n\n\n"

        + "  Benchmark a fully-buffered write of binary Ion data equivalent to example.10n both with and without "
            + "using shared symbol tables. The file tables.ion contains a sequence of Ion symbol tables.\n\n"

        + "  ion-java-benchmark write --ion-imports-for-benchmark tables.ion \\\n"
        + "                           --ion-imports-for-benchmark none \\\n"
        + "                           example.10n\n\n"

        + "  Benchmark a full-traversal read of data equivalent to exampleWithImports.10n, which declares the shared "
            + "symbol table imports provided by inputTables.ion, re-encoded (if necessary) using the shared symbol "
            + "tables provided by benchmarkTables.ion, inputTables.ion, and no shared symbol tables. Produce "
            + "results from using both the DOM and IonReader APIs.\n\n"

        + "  ion-java-benchmark read --ion-imports-for-input inputTables.ion \\\n"
        + "                          --ion-imports-for-benchmark benchmarkTables.ion \\\n"
        + "                          --ion-imports-for-benchmark auto \\\n"
        + "                          --ion-imports-for-benchmark none \\\n"
        + "                          --api dom \\\n"
        + "                          --api streaming \\\n"
        + "                          exampleWithImports.10n\n\n"

        + "  Benchmark a full-traversal read of data equivalent to example.json using both the Jackson JsonParser and "
            + "ion-java IonReader.\n\n"

        + "  ion-java-benchmark read --format json \\\n"
        + "                          --format ion_binary \\\n"
        + "                          example.json\n\n"

        + "  Benchmark a write of example.10n to JSON using Jackson ObjectMapper and to binary Ion using the"
            + "ion-java DOM. If example.10n contains Ion types that have no JSON equivalent, the data will be "
            + "down-converted using the rules provided here: "
            + "https://amzn.github.io/ion-docs/guides/cookbook.html#down-converting-to-json\n\n"

        + "  ion-java-benchmark write --format json \\\n"
        + "                           --format ion_binary \\\n"
        + "                           --api dom \\\n"
        + "                           example.10n\n\n";





    private static void printHelpAndExit(String... messages) {
        for (String message : messages) {
            System.out.println(message);
        }
        System.out.println(TITLE + DESCRIPTION + (COMMANDS + USAGE + OPTIONS).replace("\n", "\n\n") + EXAMPLES);
        System.exit(0);
    }

    static Map<String, Object> parseArguments(String... args) {
        return new Docopt(USAGE + OPTIONS)
            .withVersion(new VersionInfo().toString())
            .withHelp(false)
            .parse(args);
    }

    public static void main(String[] args) {
        Map<String, Object> optionsMap = parseArguments(args);
        if (optionsMap.get("--help").equals(true)) {
            printHelpAndExit();
        }
        try {
            if (optionsMap.get("run-suite").equals(true)) {
                GenerateAndOrganizeBenchmarkResults.generateAndSaveBenchmarkResults(optionsMap);
            } else {
                OptionsMatrixBase options = OptionsMatrixBase.from(optionsMap);
                options.executeBenchmark();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
