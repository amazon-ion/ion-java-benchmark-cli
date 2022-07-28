schema_header::{
}
type::{
    name: Customer,
    type: struct,
    fields: {
      firstName: { type: string, occurs: required },
      lastName: { type: string, occurs: optional },
      last_updated: { type: timestamp, timestamp_precision: year, occurs: required},
      addresses: {
            type:list,
            ordered_elements: [
                                   {type:string, occurs: optional},
                                   int,
                                   { type: int, occurs: range::[0, 10] },
                                 ],
      }
    },
}
schema_footer::{
}