/**
 * The RDF-Connect encoding of the Blue-bike pipeline (experiment 2, deep):
 *
 *   AnchorSource → RmlMapper (JVM) → Validate → DumpsToFeed → Sdsify
 *                → Bucketize → LdesDiskWriter
 *
 * Every stage is the published processor, configured exactly as the shell and
 * CWL arms configure it (bench/anchor2.ts, steps/*.ttl): the mapping of the
 * deployed CA-Blue-Bike-LDES pipeline, its SHACL shapes as both quality gate
 * and feed member shape, time-based fragmentation, and a static LDES on disk.
 *
 * The source sends the member shape and the focus-node query with every
 * snapshot, because DumpsToFeed consumes one of each per dump.
 */
export type BluebikePipelineConfig = {
  /** NDJSON input: one snapshot (a JSON array of stations) per line. */
  input: string
  mapping: string
  shapes: string
  /** File holding the SPARQL query that selects feed members. */
  focusQuery: string
  /** Run directory: LDES output, feed state and the source's timings go here. */
  resultDir: string
  /** Milliseconds between snapshots, for the freshness experiment. */
  arrivalMs?: number
}

export function renderBluebikePipeline(cfg: BluebikePipelineConfig): string {
  return `@prefix owl:  <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix bench:<https://w3id.org/rdf-connect/bench#>.
@prefix as:   <https://www.w3.org/ns/activitystreams#>.
@prefix tree: <https://w3id.org/tree#>.

<> owl:imports <../processors.ttl>,
  <../vendor/jvm-runner-index.jar>,
  <../rml-processor-jvm/build/libs/rml-processor-jvm-0.0.2-all.jar>,
  <../shacl-processor-ts/processors.ttl>,
  <../node_modules/@rdfc/dumps-to-feed-processor-ts/processor.ttl>,
  <../node_modules/@rdfc/sds-processors-ts/configs/sdsify.ttl>,
  <../node_modules/@rdfc/sds-processors-ts/configs/bucketizer.ttl>,
  <../node_modules/@rdfc/sds-processors-ts/configs/ldes_disk_writer.ttl>.

<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:instantiates bench:JvmRunner;
    rdfc:processor <mapper>;
  ], [
    rdfc:instantiates rdfc:NodeRunner;
    rdfc:processor <source>, <validate>, <changes>, <sdsify>, <bucketize>,
      <writer>;
  ].

# ── Channels ────────────────────────────────────────────────────────────────
<mapping> a rdfc:Writer, rdfc:Reader.
<snapshots> a rdfc:Writer, rdfc:Reader.
<mapped> a rdfc:Writer, rdfc:Reader.
<validated> a rdfc:Writer, rdfc:Reader.
<reports> a rdfc:Writer, rdfc:Reader.
<shape> a rdfc:Writer, rdfc:Reader.
<focus> a rdfc:Writer, rdfc:Reader.
<feed> a rdfc:Writer, rdfc:Reader.
<sdsData> a rdfc:Writer, rdfc:Reader.
<sdsMeta> a rdfc:Writer, rdfc:Reader.
<bucketData> a rdfc:Writer, rdfc:Reader.
<bucketMeta> a rdfc:Writer, rdfc:Reader.

# ── Stages ──────────────────────────────────────────────────────────────────
<source> a bench:BluebikeSource;
  bench:mappingWriter <mapping>;
  rdfc:writer <snapshots>;
  bench:shapeWriter <shape>;
  bench:focusWriter <focus>;
  bench:mappingPath "${cfg.mapping}";
  bench:shapePath "${cfg.shapes}";
  bench:focusPath "${cfg.focusQuery}";
  bench:input "${cfg.input}";${
    cfg.arrivalMs ? `\n  bench:arrivalMs "${cfg.arrivalMs}"^^<http://www.w3.org/2001/XMLSchema#integer>;` : ''
  }
  bench:resultPath "${cfg.resultDir}/source.json".

<mapper> a rdfc:RmlMapper;
  rdfc:mappings <mapping>;
  rdfc:source [
    rdfc:reader <snapshots>;
    rdfc:mappingId "stdin";
    rdfc:triggers true;
  ];
  rdfc:defaultTarget [
    rdfc:writer <mapped>;
    rdfc:format "nquads";
  ].

<validate> a rdfc:Validate;
  rdfc:shaclPath "${cfg.shapes}";
  rdfc:incoming <mapped>;
  rdfc:outgoing <validated>;
  rdfc:report <reports>;
  rdfc:mime "application/n-quads".

<changes> a rdfc:DumpsToFeed;
  rdfc:dump <validated>;
  rdfc:nodeShape <shape>;
  rdfc:focusNodes <focus>;
  rdfc:output <feed>;
  rdfc:feedname "bluebike";
  rdfc:flush false;
  rdfc:dumpContentType "application/n-quads";
  rdfc:focusNodesStrategy "sparql";
  rdfc:nodeShapeIri "https://blue-bike.be/shapes#ResourceReportShape";
  rdfc:dbDir "${cfg.resultDir}/feed-state/".

<sdsify> a rdfc:Sdsify;
  rdfc:input <feed>;
  rdfc:output <sdsData>;
  rdfc:metadataOutput <sdsMeta>;
  rdfc:typeFilter as:Create, as:Update, as:Delete;
  rdfc:metadataConfig [
    rdfc:streamId <https://blue-bike.be/ldes#stream>;
    rdfc:description "Feed of Blue-bike station availability updates.";
    rdfc:timestampPath as:published;
  ].

<bucketize> a rdfc:Bucketize;
  rdfc:channels [
    rdfc:dataInput <sdsData>;
    rdfc:metadataInput <sdsMeta>;
    rdfc:dataOutput <bucketData>;
    rdfc:metadataOutput <bucketMeta>;
  ];
  rdfc:bucketizeStrategy ( [
    a tree:TimebasedFragmentation;
    tree:timestampPath as:published;
    tree:maxSize 1000;
    tree:k 4;
    tree:minBucketSpan 3600;
  ] );
  rdfc:outputStreamId <https://blue-bike.be/ldes#stream>.

<writer> a rdfc:LdesDiskWriter;
  rdfc:dataInput <bucketData>;
  rdfc:metadataInput <bucketMeta>;
  rdfc:directory "${cfg.resultDir}/ldes-output";
  rdfc:streamName [
    rdfc:stream <https://blue-bike.be/ldes#stream>;
    rdfc:name "bluebike";
  ].
`
}
