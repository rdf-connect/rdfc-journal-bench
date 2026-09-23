#!/usr/bin/env nextflow

/*
 * The Blue-bike pipeline in Nextflow, for comparison with CWL and RDF-Connect.
 *
 * Nextflow is the closest competitor to a streaming system among the workflow
 * managers: a process still runs once per item, but its channels let stages
 * overlap, so snapshot 2 is mapped while snapshot 1 is validated. CWL's scatter
 * cannot do that.
 *
 * The stateful stages are the same story as in CWL. Change detection, the
 * fragmenter and the publisher each need what the previous invocation produced,
 * and a Nextflow process is an isolated task over staged inputs, so they run
 * once over the gathered stream rather than per snapshot.
 *
 * Every stage runs the same command-line tools as the other arms.
 */

nextflow.enable.dsl = 2

params.snapshots = null   // NDJSON, one snapshot per line
params.mapping   = null   // the deployed RML mapping
params.shape     = null   // member shape, one message per snapshot
params.focus     = null   // focus-node query, one message per snapshot
params.seed      = null   // empty directory: the feed state starts blank
params.steps     = null   // directory of step descriptions
params.unit      = 1      // snapshots per task in the stateless stages
params.outdir    = 'final'

/* ── Stateless stages: one task per chunk, overlapping ──────────────────── */

process MAP {
    input:
    path chunk

    output:
    path "${chunk}.mapped.ndjson"

    script:
    """
    rml-map --mapping ${params.mapping} < ${chunk} > ${chunk}.mapped.ndjson 2> ${chunk}.map.log
    """
}

process VALIDATE {
    input:
    path mapped

    output:
    path "${mapped}.valid.ndjson"

    script:
    """
    rdfc-proc --config ${params.steps}/validate-bluebike.ttl \
        --stdin in --stdout out --write report=${mapped}.report.ndjson \
        < ${mapped} > ${mapped}.valid.ndjson 2> ${mapped}.validate.log
    """
}

/* ── Stateful stages: one task over the gathered stream ─────────────────── */

process CHANGES {
    input:
    path validated
    path shape
    path focus
    path state, stageAs: 'state-in'

    output:
    path 'feed.ndjson', emit: feed
    path 'feed-state', emit: state

    script:
    """
    cp -r state-in feed-state
    rdfc-proc --config ${params.steps}/dumps-to-feed.ttl \
        --stdin in --read shape=${shape} --read focusNodes=${focus} --stdout out \
        < ${validated} > feed.ndjson 2> changes.log
    """
}

process SDSIFY {
    input:
    path feed

    output:
    path 'sds.ndjson', emit: data
    path 'sdsmeta.ndjson', emit: metadata

    script:
    """
    rdfc-proc --config ${params.steps}/sdsify-feed.ttl \
        --stdin in --stdout out --write metadata=sdsmeta.ndjson \
        < ${feed} > sds.ndjson 2> sdsify.log
    """
}

process BUCKETIZE {
    input:
    path sds
    path metadata

    output:
    path 'bucketed.ndjson', emit: data
    path 'bucketmeta.ndjson', emit: metadata

    script:
    """
    rdfc-proc --config ${params.steps}/bucketize.ttl \
        --stdin in --read metadataIn=${metadata} --stdout out --write metadataOut=bucketmeta.ndjson \
        < ${sds} > bucketed.ndjson 2> bucketize.log
    """
}

process WRITE {
    publishDir params.outdir, mode: 'copy'

    input:
    path bucketed
    path metadata

    output:
    path 'ldes-output'

    script:
    """
    rdfc-proc --config ${params.steps}/ldes-writer.ttl \
        --stdin in --read metadataIn=${metadata} \
        < ${bucketed} 2> writer.log
    """
}

workflow {
    // Nextflow does not split a file into tasks by itself either; splitText is
    // the operator that does it, and it runs in the driver rather than a task.
    chunks = Channel.fromPath(params.snapshots).splitText(by: params.unit as Integer, file: true)

    mapped = MAP(chunks)
    valid = VALIDATE(mapped)

    // Everything downstream is stateful, so the stream is gathered first, and
    // in order: a channel is unordered, and change detection compares each
    // snapshot with the one before it, so an interleaved stream invents
    // changes that never happened.
    gathered = valid.collectFile(
        name: 'validated.ndjson',
        sort: { (it.name =~ /snapshots\.(\d+)\./)[0][1] as Integer },
    )

    changes = CHANGES(gathered, file(params.shape), file(params.focus), file(params.seed))
    sds = SDSIFY(changes.feed)
    buckets = BUCKETIZE(sds.data, sds.metadata)
    WRITE(buckets.data, buckets.metadata)
}
