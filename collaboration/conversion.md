# Conversion Call Graph: conv.epip_db_to_db

## Overview

The `conv.epip_db_to_db` module transforms NeTEx data from a source MDBX database to a target MDBX database following the EPIP (European Passenger Information Profile) specifications. This document shows the call graph and how the software transforms different building blocks.

## Call Graph

```mermaid
flowchart TD
    subgraph Main["epip_db_to_db(source_db_file, target_db_file)"]
        A[Open target_db for write] --> B[Open source_db for read/write]
        B --> C[Process ServiceCalendar embeddings]
        C --> D[Process PassengerStopAssignment embeddings]
        D --> E[Copy referenced objects]
        E --> F[Copy unchanged classes]
        F --> G[Generate EPIP-specific objects]
        G --> H[Apply transformations]
        H --> I[Commit target_db]
    end

    subgraph EmbeddingResolution["1. Embedding Resolution Phase"]
        C --> C1[resolve_embeddings_iterable: ServiceCalendar]
        C1 --> C2[Extract embedded objects]
        C2 --> C3[Insert embeddings into source_db]
        C3 --> C4[Commit source_db transaction]
        
        D --> D1[resolve_embeddings_iterable: StopPlace]
        D1 --> D2[Build quay→StopPlace mapping]
        D2 --> D3[Process PSAs with quay mappings]
        D3 --> D4[Insert processed PSAs into source_db]
        D4 --> D5[Commit source_db transaction]
    end

    subgraph ReferenceCopy["2. Reference Preparation Phase"]
        E --> E1[fetch_all_references_by_class]
        E1 --> E2[Collect all objects referenced by other_referenced_classes]
        E2 --> E3[Insert referenced objects into target_db]
    end

    subgraph DirectCopy["3. Direct Copy Phase"]
        F --> F1[copy_map for Codespace]
        F --> F2[copy_map for DataSource]
        F --> F3[copy_map for ResponsibilitySet]
        F --> F4[copy_map for PassengerStopAssignment]
    end

    subgraph EPIPGeneration["4. EPIP Object Generation Phase"]
        G --> G1[epip_line_generator]
        G --> G2[infer_locations_from_quay_or_stopplace_and_apply]
        G --> G3[epip_service_journey_generator]
        G --> G4[epip_service_calendar]
        G --> G5[epip_service_journey_interchange]
    end

    subgraph Transformations["5. Transformation Phase"]
        H --> H1[infer_directions_from_sjps_and_apply]
        H --> H2[reprojection_update]
        H --> H3[avv_service_journey_operator]
        H --> H4[avv_vehicle_type_short_name]
        H --> H5[avv_quay_name]
    end

    style Main fill:#ff9,stroke:#333
    style EmbeddingResolution fill:#9f9,stroke:#333
    style ReferenceCopy fill:#99f,stroke:#333
    style DirectCopy fill:#99f,stroke:#333
    style EPIPGeneration fill:#f9f,stroke:#333
    style Transformations fill:#ff9,stroke:#333
```

## Building Block Transformations

### Input Building Blocks

```mermaid
classDiagram
    class SourceDB {
        +NeTEx GeneralFrame objects
        +ServiceCalendar with embeddings
        +PassengerStopAssignment
        +StopPlace/Quay references
        +ServiceJourneyPattern
        +Line
        +ServiceJourney
    }
```

### Transformation Pipeline

```mermaid
flowchart LR
    subgraph Input["Source DB Objects"]
        SC[ServiceCalendar]
        PSA[PassengerStopAssignment]
        SP[StopPlace]
        Q[Quay]
        Line[Line]
        SJ[ServiceJourney]
        SJP[ServiceJourneyPattern]
        SJI[ServiceJourneyInterchange]
    end

    subgraph Embedding["Embedding Extraction"]
        SC_E[ServiceCalendar
        (embeddings extracted)]
        PSA_E[PassengerStopAssignment
        (quay→StopPlace resolved)]
    end

    subgraph EPIP["EPIP Generation"]
        Line_E[Line
        (EPIP profile)]
        SJ_E[ServiceJourney
        (EPIP profile)]
        SC_E2[ServiceCalendar
        (EPIP profile)]
        SJI_E[ServiceJourneyInterchange
        (EPIP profile)]
    end

    subgraph Transform["Transformations"]
        Line_D[Line
        (directions inferred)]
        Loc[Located objects
        (reprojected)]
        SJ_O[ServiceJourney
        (operator assigned)]
    end

    subgraph Output["Target DB Objects"]
        All[Complete EPIP profile objects]
    end

    SC -->|extract embeddings| SC_E
    PSA -->|resolve quay refs| PSA_E
    Line -->|epip_line_generator| Line_E
    SJ -->|epip_service_journey_generator| SJ_E
    SC_E -->|epip_service_calendar| SC_E2
    SJP -->|epip_service_journey_interchange| SJI_E
    Line_E -->|infer_directions| Line_D
    Line_D -->|reprojection| Loc
    SJ_E -->|avv_service_journey_operator| SJ_O
    
    SC_E --> Target
    PSA_E --> Target
    Line_E --> Target
    SJ_E --> Target
    SC_E2 --> Target
    SJI_E --> Target
    Line_D --> Target
    Loc --> Target
    SJ_O --> Target
    
    Target[Target DB] --> Output
```

## Detailed Call Sequence

```mermaid
sequenceDiagram
    participant User
    participant epip_db_to_db as Main
    participant target_db as MdbxStorage
    participant source_db as MdbxStorage
    participant transformers as Transformers
    
    User->>epip_db_to_db: epip_db_to_db(source, target)
    epip_db_to_db->>target_db: __enter__(readonly=False)
    target_db-->>epip_db_to_db: txn_write
    
    epip_db_to_db->>source_db: __enter__(readonly=False)
    source_db-->>epip_db_to_db: source_db
    
    %% Embedding Resolution for ServiceCalendar
    source_db->>source_db: env.rw_transaction()
    source_db-->>epip_db_to_db: txn_write1
    epip_db_to_db->>source_db: resolve_embeddings_iterable(ServiceCalendar)
    source_db-->>epip_db_to_db: embeddings
    epip_db_to_db->>source_db: insert_any_object_on_queue(txn_write1, embeddings)
    source_db->>source_db: txn_write1.commit()
    
    %% Embedding Resolution for PassengerStopAssignment
    source_db->>source_db: env.rw_transaction()
    source_db-->>epip_db_to_db: txn_write1
    epip_db_to_db->>source_db: resolve_embeddings_iterable(StopPlace)
    source_db-->>epip_db_to_db: quay_sp mapping
    epip_db_to_db->>source_db: iter_only_objects(PassengerStopAssignment)
    source_db-->>epip_db_to_db: psa objects
    epip_db_to_db->>epip_db_to_db: Process quay references
    epip_db_to_db->>source_db: insert_any_object_on_queue(txn_write1, processed_psas)
    source_db->>source_db: txn_write1.commit()
    
    %% Reference Collection
    source_db->>source_db: env.ro_transaction()
    source_db-->>epip_db_to_db: txn_read
    epip_db_to_db->>source_db: fetch_all_references_by_class(other_referenced_classes)
    source_db-->>epip_db_to_db: referenced_objects
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, referenced_objects)
    
    %% Direct Copy
    epip_db_to_db->>source_db: copy_map(txn_read, target_db, Codespace)
    epip_db_to_db->>source_db: copy_map(txn_read, target_db, DataSource)
    epip_db_to_db->>source_db: copy_map(txn_read, target_db, ResponsibilitySet)
    epip_db_to_db->>source_db: copy_map(txn_read, target_db, PassengerStopAssignment)
    
    %% EPIP Generators
    epip_db_to_db->>transformers: epip_line_generator(source_db, txn_read, defaults)
    transformers-->>epip_db_to_db: generated_lines
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, generated_lines)
    
    epip_db_to_db->>transformers: infer_locations_from_quay_or_stopplace_and_apply(source_db, txn_read, defaults)
    transformers-->>epip_db_to_db: objects_with_locations
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, objects_with_locations)
    
    epip_db_to_db->>transformers: epip_service_journey_generator(source_db, txn_read, defaults)
    transformers-->>epip_db_to_db: epip_service_journeys
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, epip_service_journeys)
    
    epip_db_to_db->>transformers: epip_service_calendar(source_db, txn_read, defaults)
    transformers-->>epip_db_to_db: epip_service_calendars
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, epip_service_calendars)
    
    epip_db_to_db->>transformers: epip_service_journey_interchange(source_db, txn_read, defaults)
    transformers-->>epip_db_to_db: epip_service_journey_interchanges
    epip_db_to_db->>target_db: insert_any_object_on_queue(txn_write, epip_service_journey_interchanges)
    
    %% Transformations
    epip_db_to_db->>transformers: infer_directions_from_sjps_and_apply(target_db, txn_write, defaults)
    epip_db_to_db->>transformers: reprojection_update(target_db, txn_write, "urn:ogc:def:crs:EPSG::4326")
    epip_db_to_db->>transformers: avv_service_journey_operator(target_db, txn_write)
    epip_db_to_db->>transformers: avv_vehicle_type_short_name(target_db, txn_write)
    epip_db_to_db->>transformers: avv_quay_name(target_db, txn_write)
    
    epip_db_to_db->>target_db: txn_write.commit()
    source_db->>source_db: __exit__()
    target_db->>target_db: __exit__()
```

## Data Flow Diagram

```mermaid
flowchart TD
    subgraph SourceDB["Source Database"]
        S1[ServiceCalendar
        with embedded objects]
        S2[PassengerStopAssignment
        with quay references]
        S3[StopPlace/Quay
        objects]
        S4[Line objects]
        S5[ServiceJourneyPattern]
        S6[ServiceJourney]
        S7[Other referenced objects]
    end

    subgraph Processing["Processing Functions"]
        P1[resolve_embeddings_iterable
        Extract embedded ServiceCalendar objects]
        P2[resolve_embeddings_iterable
        Build quay→StopPlace mapping]
        P3[fetch_all_references_by_class
        Collect referenced objects]
        P4[copy_map
        Direct copy unchanged classes]
        P5[epip_line_generator
        Create EPIP-compliant Lines]
        P6[infer_locations
        Add coordinates from quay/stopplace]
        P7[epip_service_journey_generator
        Create EPIP ServiceJourneys]
        P8[epip_service_calendar
        Create EPIP ServiceCalendars]
        P9[epip_service_journey_interchange
        Create EPIP ServiceJourneyInterchanges]
        P10[infer_directions
        Infer direction from patterns]
        P11[reprojection
        Project to target CRS]
        P12[avv_* functions
        IVU-specific transformations]
    end

    subgraph TargetDB["Target Database"]
        T1[Referenced objects
        (from source)]
        T2[Copied classes
        (Codespace, DataSource, etc.)]
        T3[Generated EPIP Lines]
        T4[Objects with locations]
        T5[Generated EPIP ServiceJourneys]
        T6[Generated EPIP ServiceCalendars]
        T7[Generated EPIP ServiceJourneyInterchanges]
        T8[Lines with directions]
        T9[Reprojected objects]
        T10[IVU-enhanced objects]
    end

    S1 --> P1
    S3 --> P2
    S2 --> P2
    S7 --> P3
    S4 --> P5
    S3 --> P6
    S5 --> P7
    S6 --> P7
    S6 --> P8
    S5 --> P8
    S6 --> P9
    T3 --> P10
    T4 --> P11
    T5 --> P12

    P1 --> S1
    P2 --> T1
    P3 --> T1
    P4 --> T2
    P5 --> T3
    P6 --> T4
    P7 --> T5
    P8 --> T6
    P9 --> T7
    P10 --> T8
    P11 --> T9
    P12 --> T10
```

## Key Transformers Involved

| Transformer | Source | Purpose | Output |
|-------------|--------|---------|--------|
| `epip_line_generator` | `transformers.epip` | Create EPIP-compliant Line objects | EPIP Line objects |
| `infer_locations_from_quay_or_stopplace_and_apply` | `transformers.scheduledstoppoint` | Add coordinates from Quay/StopPlace | Objects with explicit locations |
| `epip_service_journey_generator` | `transformers.epip` | Generate EPIP ServiceJourneys from patterns | EPIP ServiceJourney objects |
| `epip_service_calendar` | `transformers.epip` | Create EPIP ServiceCalendars | EPIP ServiceCalendar objects |
| `epip_service_journey_interchange` | `transformers.epip` | Generate ServiceJourneyInterchanges | EPIP ServiceJourneyInterchange objects |
| `infer_directions_from_sjps_and_apply` | `transformers.direction` | Infer Direction from ServiceJourneyPatterns | Lines/ServiceJourneys with Direction |
| `reprojection_update` | `transformers.projection` | Reproject all locations to target CRS | Objects with reprojected coordinates |
| `avv_service_journey_operator` | `transformers.ivu` | Assign operators (IVU-specific) | ServiceJourneys with operators |
| `avv_vehicle_type_short_name` | `transformers.ivu` | Fix vehicle type names | VehicleTypes with short names |
| `avv_quay_name` | `transformers.ivu` | Fix quay names | Quays with proper names |

## Building Block Dependency Graph

```mermaid
graph TD
    ServiceCalendar -->|embeddings extracted| ServiceCalendar
    PassengerStopAssignment -->|quay refs resolved| StopPlace
    StopPlace -->|coordinates| ScheduledStopPoint
    Line -->|EPIP rules| EPIP_Line
    ServiceJourneyPattern -->|EPIP rules| EPIP_ServiceJourney
    ServiceJourney -->|EPIP rules| EPIP_ServiceJourney
    ServiceJourneyPattern -->|interchange rules| ServiceJourneyInterchange
    EPIP_Line -->|direction inference| Line_with_Direction
    Line_with_Direction -->|reprojection| Line_with_Projection
    EPIP_ServiceJourney -->|operator assignment| ServiceJourney_with_Operator
```

## Performance Characteristics

| Phase | Database Access | CPU Intensive | Memory Usage | I/O Pattern |
|-------|-----------------|---------------|--------------|-------------|
| Embedding Resolution | Read/Write source | Medium | Medium | Sequential |
| Reference Collection | Read source | Low | Low | Random |
| Direct Copy | Read source, Write target | Low | Low | Sequential |
| EPIP Generation | Read source, Write target | High | High | Random |
| Transformations | Read/Write target | Medium | Medium | Random |

## Summary

The `conv.epip_db_to_db` call graph demonstrates a well-structured transformation pipeline that:

1. **Prepares** the source data by extracting embeddings and resolving references
2. **Copies** unchanged objects directly to the target
3. **Generates** EPIP-specific objects using profile transformers
4. **Applies** additional transformations (directions, projections, IVU-specific rules)

Each phase operates on specific building blocks, transforming them according to the EPIP profile requirements while maintaining the integrity of the NeTEx data model.