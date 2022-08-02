
 schema_header::{
 }
 type::{
       name: List,
              type: list,
              ordered_elements: [
                  string,
                  int,
                   { type: int, occurs: range::[0, 10] },
                ]
            }

  schema_footer::{
  }