 schema_header::{
 }
 type::{
      name: Customer,
      type: struct,
      annotations:[corporate, gold_class, club_member],
      fields: {
        firstName: { type: string, occurs: required },
        lastName: { type: string, occurs: optional },
        last_updated: { type: timestamp, timestamp_precision: year, occurs: required},
      },
 }
  schema_footer::{
  }