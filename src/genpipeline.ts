/**
 * Emits the pipeline.ttl for arm A of experiment 1.
 *
 * The generated pipeline instantiates exactly the processors the hand-wired
 * driver instantiates, with the same parameters and in the same order, so the
 * two arms differ only in how they are executed.
 */
export type PipelineConfig = {
  count: number
  size: number
  mode: string
  depth: number
  resultDir: string
  /** Microseconds of CPU work each consuming processor spends per message. */
  workUs?: number
  /**
   * 'single' = all processors in one runner (`npx js-runner`)
   * 'split'  = generator side and sink in separate runner processes
   * 'direct' = one runner, invoked as `node .../runner.js` instead of via npx
   */
  placement?: 'single' | 'split' | 'direct'
  /** See BenchGenerator.lingerMs; needed when the generator's runner would otherwise exit immediately. */
  lingerMs?: number
}

export function renderPipeline(cfg: PipelineConfig): string {
  const placement = cfg.placement ?? 'single'
  const nChannels = cfg.depth + 1

  const procNames = [
    'gen',
    ...Array.from({ length: cfg.depth }, (_, i) => `pt${i}`),
    'sink',
  ]

  const channels = Array.from(
    { length: nChannels },
    (_, i) => `<ch${i}> a rdfc:Writer, rdfc:Reader.`,
  ).join('\n')

  // Runner groups.
  let consistsOf: string
  if (placement === 'split') {
    // Generator (and any pass-throughs) in runner A, sink in runner B.
    const groupA = procNames.slice(0, -1).map((n) => `<${n}>`).join(', ')
    consistsOf = `  rdfc:consistsOf [
    rdfc:instantiates bench:NodeRunnerA;
    rdfc:processor ${groupA};
  ], [
    rdfc:instantiates bench:NodeRunnerB;
    rdfc:processor <sink>;
  ].`
  } else {
    const all = procNames.map((n) => `<${n}>`).join(', ')
    const runner =
      placement === 'direct' ? 'bench:NodeRunnerDirect' : 'rdfc:NodeRunner'
    consistsOf = `  rdfc:consistsOf [
    rdfc:instantiates ${runner};
    rdfc:processor ${all};
  ].`
  }

  const workLine = cfg.workUs
    ? `;\n  bench:workUs "${cfg.workUs}"^^xsd:integer`
    : ''

  const passthroughs = Array.from(
    { length: cfg.depth },
    (_, i) => `<pt${i}> a bench:BenchPassThrough;
  rdfc:reader <ch${i}>;
  rdfc:writer <ch${i + 1}>${workLine}.`,
  ).join('\n\n')

  return `@prefix owl:  <http://www.w3.org/2002/07/owl#>.
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix bench:<https://w3id.org/rdf-connect/bench#>.

<> owl:imports <../processors.ttl>.

<> a rdfc:Pipeline;
${consistsOf}

${channels}

<gen> a bench:BenchGenerator;
  rdfc:writer <ch0>;
  bench:count "${cfg.count}"^^xsd:integer;
  bench:size "${cfg.size}"^^xsd:integer;
  bench:mode "${cfg.mode}";
  bench:resultPath "${cfg.resultDir}/generator.json"${
    cfg.lingerMs ? `;
  bench:lingerMs "${cfg.lingerMs}"^^xsd:integer` : ''
  }.

${passthroughs}

<sink> a bench:BenchSink;
  rdfc:reader <ch${nChannels - 1}>;
  bench:resultPath "${cfg.resultDir}/sink.json"${workLine}.
`
}

export type AnchorPipelineConfig = {
  /** NDJSON input, one unit per line (bench/anchor.ts). */
  input: string
  mapping: string
  shapes: string
  resultDir: string
}

/**
 * Emits the pipeline.ttl for the RDF-Connect arm of experiment 2:
 * AnchorSource → RmlMapper (JVM runner) → Validate (js-runner) → AnchorSink,
 * with Validate's report channel going to a second AnchorSink.
 *
 * The mapper and validator are the published processors, loaded from the
 * local checkouts the CLIs are built from. Paths are absolute: processor
 * arguments resolve against the orchestrator's working directory.
 */
export function renderAnchorPipeline(cfg: AnchorPipelineConfig): string {
  return `@prefix owl:  <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix bench:<https://w3id.org/rdf-connect/bench#>.

<> owl:imports <../processors.ttl>,
  <../vendor/jvm-runner-index.jar>,
  <rml-index-only.ttl>,
  <../shacl-processor-ts/processors.ttl>.

<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:instantiates bench:JvmRunner;
    rdfc:processor <rml>;
  ], [
    rdfc:instantiates rdfc:NodeRunner;
    rdfc:processor <source>, <shacl>, <sink>, <reportSink>;
  ].

<mapping> a rdfc:Writer, rdfc:Reader.
<records> a rdfc:Writer, rdfc:Reader.
<mapped> a rdfc:Writer, rdfc:Reader.
<valid> a rdfc:Writer, rdfc:Reader.
<reports> a rdfc:Writer, rdfc:Reader.

<source> a bench:AnchorSource;
  bench:mappingWriter <mapping>;
  rdfc:writer <records>;
  bench:mappingPath "${cfg.mapping}";
  bench:input "${cfg.input}";
  bench:resultPath "${cfg.resultDir}/source.json".

<rml> a rdfc:RmlMapper;
  rdfc:mappings <mapping>;
  rdfc:source [
    rdfc:reader <records>;
    rdfc:mappingId "stdin";
    rdfc:triggers true;
  ];
  rdfc:defaultTarget [
    rdfc:writer <mapped>;
    rdfc:format "nquads";
  ].

<shacl> a rdfc:Validate;
  rdfc:shaclPath "${cfg.shapes}";
  rdfc:incoming <mapped>;
  rdfc:outgoing <valid>;
  rdfc:report <reports>;
  rdfc:mime "application/n-quads".

<sink> a bench:AnchorSink;
  rdfc:reader <valid>;
  bench:outPath "${cfg.resultDir}/out.ndjson";
  bench:resultPath "${cfg.resultDir}/sink.json".

<reportSink> a bench:AnchorSink;
  rdfc:reader <reports>;
  bench:outPath "${cfg.resultDir}/report.ndjson";
  bench:resultPath "${cfg.resultDir}/report-sink.json".
`
}
