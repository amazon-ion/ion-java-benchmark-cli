$ion_schema_1_0

schema_header::{
  imports: [
    { id: "com/example/util_types.isl", type: Address },
  ],
}

type::{
  name: Customer,
  type: struct,
  Annotations:required::[corporate, gold_class, club_member],
  fields: {
    firstName: { type: string, occurs: required, codepoint_length:[10,20] },
    lastName: { type: string, occurs: optional },
    last_updated: {
      type: timestamp,
      timestamp_precision: year,
      occurs: required,
    },
  },
}

schema_footer::{
}