package com.amazon.ion.benchmark;

import org.docopt.Docopt;

import java.util.Map;

public class Main {

    private static final String TITLE = "IonJava Benchmarking Tool\n\n";

    private static final String DESCRIPTION =
        "Description:\n"
        + "  Tool that allows users to...\n"
        + "    1. Compare IonJava to Java implementations of other serialization formats (not yet implemented)\n"
        + "    2. Determine which IonJava configurations perform best\n"
        + "  ...for the individual users' data and access patterns.\n"
        + "  Additionally, allows IonJava developers to...\n"
        + "    1. Determine the impact of a proposed change\n"
        + "    2. Decide where investments should be made in improving performance\n"
        + "  ...by generating results from a variety of real-world data and access patterns.\n"
        + "\n";

    private static final String USAGE =
        "Usage:\n"
        + "  ion-java-benchmark write [--profile] [--io-type <type>] [--ion-api <api>]... [--format <type>]... "
                + "[--limit <int>] [--warmups <int>] [--iterations <int>] [--forks <int>] [--results-format <type>] "
                + "[--results-file <file] [--ion-imports <file>] [--ion-flush-period <int>]... "
                + "[--ion-length-preallocation <int>]... <input_file>\n"
        + "  ion-java-benchmark read [--profile] [--io-type <type>] [--ion-api <api>]... [--format <type>]... "
                + "[--limit <int>] [--warmups <int>] [--iterations <int>] [--forks <int>] [--results-format <type>] "
                + "[--results-file <file>] [--ion-imports <file>] [--ion-flush-period <int>]... "
                + "[--ion-length-preallocation <int>]... [--ion-reader <type>]... [--paths <file>] <input_file>\n"
        + "  ion-java-benchmark --help\n"
        + "  ion-java-benchmark --version\n"
        + "\n";

    private static final String COMMANDS =
        "Commands:\n"
        + "  write    Benchmark writing the given input file to the given output format(s) . In order to isolate "
                + "writing from reading, during the setup phase write instructions are generated from the input file "
                + "and stored in memory. For large inputs, this can consume a lot of resources and take a long time "
                + "to execute. This may be reduced by using the --limit option to limit the number of entries that "
                + "are written.\n"
        + "  read     First, re-write the given input file to the given output format(s) (if necessary), then "
                + "benchmark reading the resulting log files. If this takes too long to complete, consider using "
                + "the --limit option to limit the number of entries that are read. Specifying non-default settings "
                + "for certain options will cause the input data to be re-encoded even if the requested format is the "
                + "same as the format of the provided input. These options are --ion-length-preallocation and "
                + "--ion-flush-period for input in the ion binary format.\n"
        + "\n";

    // TODO IO buffer size (for buffered input/output streams)
    // TODO ion float 32 support
    // TODO read symbols as SymbolToken instead of String
    // TODO read lobs using getBytes() into reusable buffers
    // TODO read decimals using bigDecimalValue() instead of decimalValue()
    // TODO binary writer block size
    private static final String OPTIONS =
        "Options:\n"
        + "  -h --help                             Show this screen.\n"
        + "  -v --version                          Show the version of this tool and ion-java.\n"
        + "  -o --results-file <path>              Destination for the benchmark results. By default, results will be "
                + "written to stdout unless a results format other than jmh is specified, in which case the results "
                + "will be written to a file with the default name 'jmh-result'.\n"
        + "  -r --results-format <type>            Format for the benchmark results, from the set (jmh | ion). "
                + "Specifying an option other than jmh will cause the results to be written to a file. "
                + "[default: jmh]\n"
        + "  -f --format <type>                    Format to benchmark, from the set (ion_binary | ion_text). May be "
                + "specified multiple times to compare different formats. [default: ion_binary]\n"
        + "  -n --limit <int>                      Maximum number of entries to process. By default, all entries in "
                + "each input file are processed.\n"
        + "  -t --io-type <type>                   The source or destination type, from the set (buffer | file). If "
                + "buffer is selected, buffers the input data in memory before reading and writes the output data to "
                + "an in-memory buffer instead of a file. To limit the amount of memory required, use --limit. "
                + "[default: file]\n"
        + "  -w --warmups <int>                    Number of benchmark warm-up iterations. [default: 10]\n"
        + "  -i --iterations <int>                 Number of benchmark iterations. [default: 10]\n"
        + "  -F --forks <int>                      Number of benchmark forks (distinct JVMs). [default: 1]\n"
        + "  -p --profile                          Initiates a single iteration that repeats indefinitely until "
                + "terminated, allowing users to attach profiling tools. If this option is specified, the --warmups, "
                + "--iterations, and --forks options are ignored. An error will be raised if this option is used when "
                + "multiple values are specified for other options. Not enabled by default.\n"
        + "  -a --ion-api <api>                    The Ion API to exercise (dom or streaming). Ignored unless one of "
                + "the specified --formats is ion-binary or ion-text. May be specified multiple times to compare both "
                + "APIs. [default: streaming]\n"
        + "  -c --ion-imports <file>               A file containing a sequence of Ion symbol tables. Any Ion data "
                + "written will use these as shared symbol tables. Any Ion data read will use these as a catalog from "
                + "which to resolve shared symbol table imports. Ignored unless one of the specified formats is "
                + "ion-binary or ion-text. By default, no shared symbol tables are used.\n"
        + "  -L --ion-length-preallocation <int>   Number of bytes that the Ion writer will preallocate for length "
                + "fields, from the set (0, 1, 2, auto). Lower numbers lead to a more compact encoding; higher numbers "
                + "lead to faster writing. May be specified multiple times to compare different settings. The 'auto' "
                + "setting behaves is as follows: for read benchmarks, if the other settings make it possible to use "
                + "the input data as-is, the input will not be re-encoded. If other settings require the input to be "
                + "re-encoded, then 2-byte preallocation will be used. For write benchmarks, 'auto' causes 2-byte "
                + "preallocation to be used. [default: auto]\n"
        + "  -d --ion-flush-period <int>           The number of top-level values to write between flushes, or 'auto'. "
                + "Each flush initiates a new local symbol table append. Ignored unless one of the specified formats "
                + "is ion-binary. May be specified multiple times to compare multiple settings. The 'auto' setting "
                + "behaves as follows: for read benchmarks, if the other settings make it possible to use the input "
                + "data as-is, the input will not be re-encoded. If other settings require the input to be re-encoded, "
                + "then flushes will occur at every symbol table encountered in the input, preserving the existing "
                + "symbol table boundaries. For write benchmarks, no incremental flushes will be performed; the entire "
                + "output will be buffered until writing completes. [default: auto]\n"
        + "  -R --ion-reader <type>                The IonReader type to use, from the set (blocking | non_blocking). "
                + "May be specified multiple times to compare different readers. [default: non_blocking]\n"
        + "  -s --paths <file>                     A file containing a sequence of Ion s-expressions representing "
                + "search paths (https://github.com/amzn/ion-java-path-extraction/#search-paths) into the input data. "
                + "Only values matching one of the paths will be materialized; all other values will be skipped. For "
                + "Ion data, registering at least one search path causes the ion-java-path-extraction "
                + "(https://github.com/amzn/ion-java-path-extraction/) extension to ion-java to be used. For other "
                + "formats, the most efficient known API for performing sparse reads will be used. Ignored unless "
                + "--ion-api streaming is specified. By default, no search paths will be used, meaning the input data "
                + "will be fully traversed and materialized.)\n"
        + "\n";

    private static final String EXAMPLES =
        "Examples:\n" +
        "  Benchmark a full-traversal read of example.10n from file using the IonReader API, with 10 warmups, 10 " +
                "iterations, and 1 fork, printing the results to stdout in JMH’s standard text format.\n" +
        "\n" +
        "  ion-java-benchmark read example.10n\n" +
        "\n" +
        "  Benchmark a fully-buffered write of binary Ion data equivalent to example.10n to file using the " +
                "IonWriter API, with 10 warmups, 10 iterations, and 1 fork, printing the results to stdout in " +
                "JMH’s standard text format.\n" +
        "\n" +
        "  ion-java-benchmark write example.10n\n" +
        "\n" +
        "  Benchmark a write of binary Ion data equivalent to the first 1,000 top-level values in example.10n to " +
                "in-memory bytes using the IonWriter API, flushing after every 100 top-level values. Produce " +
                "results for both 0-byte length preallocation and 2-byte length preallocation to facilitate " +
                "comparison of both settings.\n" +
        "\n" +
        "  ion-java-benchmark write --io-type buffer \\ \n" +
        "                         --limit 1000 \\\n" +
        "                         --ion-flush-period 100 \\\n" +
        "                         --ion-length-preallocation 0 \\\n" +
        "                         --ion-length-preallocation 2 \\ \n" +
        "                         example.10n\n" +
        "\n" +
        "  Profile a sparse read of example.10n from file, materializing only the values that match the paths " +
                "specified in paths.ion,  using ion-java-path-extraction. This process will repetitively execute " +
                "until manually terminated, allowing the user to attach a tool for gathering performance profiles.\n" +
        "\n" +
        "  ion-java-benchmark read --profile --paths paths.ion example.10n\n" +
        "\n" +
        "  Benchmark a fully-buffered write of binary Ion data equivalent to example.10n both with and without " +
                "using shared symbol tables. The file tables.ion contains a sequence of Ion symbol tables, while " +
                "the file empty.ion is empty.\n" +
        "\n" +
        "  ion-java-benchmark write --ion-imports tables.ion --ion-imports empty.ion example.10n\n" +
        "\n" +
        "  Benchmark a full-traversal read of example.10n, producing results from using both the DOM and IonReader " +
                "APIs, using the sequence of symbol tables specified in tables.ion as the catalog from which to " +
                "resolve shared symbol table imports encountered in the input data.\n" +
        "\n" +
        "  ion-java-benchmark read --ion-imports tables.ion \\\n" +
        "                        --ion-api dom \\\n" +
        "                        --ion-api streaming \\\n" +
        "                        example.10n\n" +
        "\n" +
        "  Benchmark a fully-buffered write of data equivalent to example.10n to file, producing results for both " +
                "binary Ion and CBOR output. Write the benchmark results to results.ion in the text Ion format.\n" +
        "\n" +
        "  ion-java-benchmark write --format ion_binary \\\n" +
        "                         --format cbor \\\n" +
        "                         --results-format ion \\\n" +
        "                         --results-file results.ion \\\n" +
        "                         example.10n\n" +
        "\n" +
        "  Benchmark a full-traversal read of data equivalent to the first 1,000 top-level values in example.10n " +
                "from both binary Ion and Protobuf files, using 20 warmups, 5 iterations, and 2 forks.\n" +
        "\n" +
        "  ion-java-benchmark read --limit 1000 \\\n" +
        "                        --format ion_binary \\\n" +
        "                        --format protobuf \\\n" +
        "                        --warmups 20 \\\n" +
        "                        --iterations 5 \\\n" +
        "                        --forks 2 \\\n" +
        "                        example.10n\n" +
        "\n";

    private static void printHelpAndExit(String... messages) {
        for (String message : messages) {
            System.out.println(message);
        }
        System.out.println(TITLE + DESCRIPTION + COMMANDS + USAGE + OPTIONS + EXAMPLES);
        System.exit(0);
    }

    public static void main(String[] args) {
        Map<String, Object> optionsMap = new Docopt(USAGE + OPTIONS)
            .withVersion(new VersionInfo().toString())
            .withHelp(false)
            .parse(args);
        if (optionsMap.get("--help").equals(true)) {
            printHelpAndExit();
        }
        try {
            OptionsCombinationsBase options = OptionsCombinationsBase.from(optionsMap);
            options.executeBenchmark();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
