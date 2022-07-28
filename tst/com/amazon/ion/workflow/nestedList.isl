 type::{
       name: nestedList,
              type: list,
              ordered_elements: [
                  string,
                  int,
                   { type: int, occurs: range::[0, 10] },
                   {type: struct,
                         fields: {
                         first_name: string,
                         last_name: string,
                         last_updated: { type: timestamp, timestamp_precision: year, occurs: range:: [2, 5]},
                         }
                   },
                   {type: list,
                   ordered_elements: [string, {type: struct, container_length:3, element: string}]
                   },
                ]
 }