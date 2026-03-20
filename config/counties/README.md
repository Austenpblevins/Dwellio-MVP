# County Adapter Config

Each county file defines adapter-level configuration metadata.

Current scope:
- keep files declarative
- no parser implementation logic
- no county-specific feature logic

Stage 5 status:
- Harris and Fort Bend both use the shared ingestion framework through county adapter/config layers
- dataset registry and field mapping files under each county directory are the reviewable source of county-specific ingestion metadata

Current configured counties:
- `harris.yaml`
- `fort_bend.yaml`
