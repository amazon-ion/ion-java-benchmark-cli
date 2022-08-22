type::{
    name:any_of_test,
    any_of:[
        {
            type:string,
            codepoint_length:19
        },
        {
            type:struct,
            fields:{
                    StartTime:{type:timestamp},
                    'ACK.count':{type:string,codepoint_length:1},
                    'ACK.re-msid':{type:string,codepoint_length:36}
            }
        }
    ]
}




