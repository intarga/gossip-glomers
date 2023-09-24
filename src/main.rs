use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum MsgBody {
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Echo {
        msg_id: usize,
        echo: String,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
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

    let mut msg_id_gen = MsgIdGen::new();

    loop {
        eprintln!("pre des");
        let in_msg = Msg::deserialize(&mut de)?;
        eprintln!("des ok");

        let out_body = match in_msg.body {
            MsgBody::Init {
                msg_id,
                node_id: _,
                node_ids: _,
            } => MsgBody::InitOk {
                msg_id: msg_id_gen.gen(),
                in_reply_to: msg_id,
            },
            MsgBody::Echo { msg_id, echo } => MsgBody::EchoOk {
                msg_id: msg_id_gen.gen(),
                in_reply_to: msg_id,
                echo,
            },
            _ => panic!(),
        };
        let out_msg = Msg {
            src: in_msg.dst,
            dst: in_msg.src,
            body: out_body,
        };
        eprintln!("reply constructed");

        println!("{}", serde_json::to_string(&out_msg)?);

        eprintln!("reply sent");
    }
}
