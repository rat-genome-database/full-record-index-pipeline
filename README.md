# full-record-index-pipeline

Precomputes summaries from Phenominer experiment record data into the `FULL_RECORD_INDEX`
table, which powers fast lookups by the Phenominer tool. For every record, the pipeline
expands each ontology term into all of its active ancestors so queries on a parent term
will match any descendant annotated to a record.

## What it does

For each study → experiment → record (where `curation_status = 40`), the pipeline
indexes the following term accessions together with their active ancestors:

- measurement method
- clinical measurement
- sample strain
- every condition on the record

Each resulting `(experiment_record_id, term_acc, primary_term_acc, aspect, study_id,
study_name, experiment_id, experiment_name)` tuple is compared against the current
contents of `FULL_RECORD_INDEX`:

- rows already present have their `last_update_date` refreshed
- new rows are inserted
- rows in RGD but not in the incoming set are deleted
- after processing, rows whose `last_update_date` is older than the pipeline start
  time are deleted as stale — with a safety cap: if the stale set would exceed 10% of
  the incoming row count, the stale cleanup is aborted and a warning is logged

Studies are processed in parallel via `parallelStream()`.

## Running

```
./run.sh
```

Connection details are supplied via `-Dspring.config=.../default_db2.xml`; logging is
configured via `-Dlog4j.configurationFile=.../log4j2.xml`. See `src/main/dist/run.sh`.

## Build

```
./gradlew clean build
```

Produces a runnable distribution under `build/distributions/`.

## Logs

- `core` — pipeline status and summary counts
- `incoming` — dump of every incoming full-record tuple
- `inserted` — rows written to `FULL_RECORD_INDEX`
- `deleted` — rows removed from `FULL_RECORD_INDEX`
