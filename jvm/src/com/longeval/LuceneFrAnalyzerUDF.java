package com.longeval;

import io.anserini.analysis.AnalyzerUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.spark.sql.api.java.UDF1;

import java.util.List;

/**
 * Native Spark UDF that tokenizes French text with the exact same Lucene
 * analyzer pyserini uses, but inside the executor JVM instead of an
 * embedded pyjnius JVM per Python worker.
 *
 * <p>Equivalence is not a reimplementation: this calls the identical
 * bytecode pyserini's {@code Analyzer.analyze} does —
 * {@code io.anserini.analysis.AnalyzerUtils.analyze(analyzer, text)} with
 * {@code org.apache.lucene.analysis.fr.FrenchAnalyzer} (default ctor,
 * default FR stopword set) — both shipped in anserini's fatjar, which is
 * what pyserini loads. Output is byte-identical to the old
 * {@code _analyze} UDF.
 *
 * <p>One analyzer per executor JVM. Lucene {@link Analyzer} is designed
 * to be shared across threads; {@code AnalyzerUtils.analyze} builds a
 * fresh TokenStream per call, so the static instance is safe under
 * Spark's per-task threads.
 */
public final class LuceneFrAnalyzerUDF implements UDF1<String, String[]> {

    private static final Analyzer ANALYZER = new FrenchAnalyzer();

    @Override
    public String[] call(String text) {
        // Mirror pyserini's _analyze: a null/None input analyzes "".
        List<String> tokens = AnalyzerUtils.analyze(ANALYZER, text == null ? "" : text);
        return tokens.toArray(new String[0]);
    }
}
