# Initial domain model

## Tenant boundary

Every persisted object is owned by an `organization_id`. Cross-organization reads are forbidden by application authorization and database policy. A future platform administrator may have explicitly audited cross-tenant capabilities, but ordinary users never do.

## Core entities

- Organization
- Facility
- Supplier
- Material
- Batch
- Shipment
- Inventory lot
- Manufacturing order
- Finished product
- Customer or distribution destination
- Source document
- Evidence claim
- Investigation

## Canonical event types

- `RECEIVE`
- `SHIP`
- `STORE`
- `TRANSFORM`
- `SPLIT`
- `MERGE`
- `CONSUME`
- `PRODUCE`
- `INSPECT`
- `QUARANTINE`

## Event requirements

Every event must carry:

- organization and source-system identity;
- event type and event time, with time precision;
- source and destination entities where applicable;
- material/batch and quantity information where applicable;
- source document or system record references;
- extraction and validation status;
- version, ingestion timestamp, and idempotency key.

## Relationship states

Relationships are explicitly classified as:

- `VERIFIED` - supported by validated evidence;
- `PROBABLE` - strong multi-signal match requiring review policy;
- `POSSIBLE` - candidate relationship, not safe for recall decisions;
- `CONTRADICTED` - conflicting evidence exists;
- `UNKNOWN` - the available evidence is insufficient.
