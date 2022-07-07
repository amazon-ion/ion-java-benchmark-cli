 schema_header::{
 }
 type::{
      name: Customer,
      type: struct,
      fields: {
              first_name: string,
              last_name: string,
              last_updated: { type: timestamp, timestamp_precision: year, occurs: range:: [2, 5]},
      },
 }
  schema_footer::{
  }