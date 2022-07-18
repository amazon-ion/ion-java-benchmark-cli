type::{
    name: TestAnnotations,
    type: struct,
    annotations:[struct, nestedStruct],
    fields: {
      firstName: { type: string, occurs: required, annotations: required::[test] },
      lastName: { type: string, occurs: optional, annotations: ordered::[test, ordered, annotations]  },
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