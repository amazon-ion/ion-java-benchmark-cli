type::{
    name:one_of_test,
    one_of:[
        {
            type:string,
            codepoint_length:19
        },
        int,
        {
            type:struct,
            fields: {
                Operation:{one_of:[{type:string,codepoint_length:range::[6,16]},symbol]},
                PID:{one_of:[{type:string,codepoint_length:59},symbol]}
            }
        }
    ]
}




