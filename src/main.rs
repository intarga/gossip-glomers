use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum ExtraFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MsgBody {
    msg_id: usize,
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    extra: ExtraFields,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Msg {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: MsgBody,
}

struct MsgIdGen(usize);

impl MsgIdGen {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn gen(&mut self) -> usize {
        self.0 += 1;
        self.0
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let in_handle = stdin.lock();
    let mut de = serde_json::Deserializer::from_reader(in_handle);

    let mut curr_node_id: Option<String> = None;

    let mut msg_id_gen = MsgIdGen::new();

    loop {
        let in_msg = Msg::deserialize(&mut de)?;

        let msg_id = msg_id_gen.gen();

        let out_inner = match in_msg.body.extra {
            ExtraFields::Init {
                node_id,
                node_ids: _,
            } => {
                curr_node_id = Some(node_id);
                ExtraFields::InitOk
            }
            ExtraFields::Echo { echo } => ExtraFields::EchoOk { echo },
            ExtraFields::Generate => ExtraFields::GenerateOk {
                id: format!("{}-{}", curr_node_id.clone().unwrap_or_default(), msg_id),
            },
            _ => panic!(),
        };
        let out_msg = Msg {
            src: in_msg.dst,
            dst: in_msg.src,
            body: MsgBody {
                msg_id,
                in_reply_to: Some(in_msg.body.msg_id),
                extra: out_inner,
            },
        };

        println!("{}", serde_json::to_string(&out_msg)?);
    }
}
