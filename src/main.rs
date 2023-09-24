use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io,
};

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
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
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

struct NodeData {
    node_id: Option<String>,
    node_ids: Option<Vec<String>>,
    messages: HashSet<usize>,
    topology: Option<HashMap<String, Vec<String>>>,
}

impl NodeData {
    pub fn new() -> Self {
        Self {
            node_id: None,
            node_ids: None,
            messages: HashSet::new(),
            topology: None,
        }
    }

    fn neighbours_from_topology(&self) -> Option<Vec<String>> {
        Some(
            self.topology
                .clone()?
                .get(&(self.node_id.clone()?))?
                .clone(),
        )
    }

    pub fn get_neighbours(&self) -> Option<Vec<String>> {
        Some(
            self.neighbours_from_topology()
                .unwrap_or(self.node_ids.clone()?),
        )
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let in_handle = stdin.lock();
    let mut de = serde_json::Deserializer::from_reader(in_handle);

    let mut node_data = NodeData::new();

    let mut msg_id_gen = MsgIdGen::new();

    loop {
        let in_msg = Msg::deserialize(&mut de)?;

        let msg_id = msg_id_gen.gen();

        let ok_extra: Option<ExtraFields> = match in_msg.body.extra {
            ExtraFields::Init { node_id, node_ids } => {
                node_data.node_id = Some(node_id);
                node_data.node_ids = Some(node_ids);
                Some(ExtraFields::InitOk)
            }
            ExtraFields::InitOk => None,
            ExtraFields::Echo { echo } => Some(ExtraFields::EchoOk { echo }),
            ExtraFields::EchoOk { echo: _ } => None,
            ExtraFields::Generate => Some(ExtraFields::GenerateOk {
                id: format!(
                    "{}-{}",
                    node_data.node_id.clone().unwrap_or_default(),
                    msg_id
                ),
            }),
            ExtraFields::GenerateOk { id: _ } => None,
            ExtraFields::Broadcast { message } => {
                let is_new = node_data.messages.insert(message);
                if is_new {
                    if let Some(neighbours) = node_data.get_neighbours() {
                        for neighbour in neighbours {
                            let fwd_msg = Msg {
                                src: node_data.node_id.clone().unwrap(),
                                dst: neighbour,
                                body: MsgBody {
                                    msg_id: msg_id_gen.gen(),
                                    in_reply_to: None,
                                    extra: ExtraFields::Broadcast { message },
                                },
                            };
                            println!("{}", serde_json::to_string(&fwd_msg)?);
                        }
                    }
                }
                Some(ExtraFields::BroadcastOk)
            }
            ExtraFields::BroadcastOk => None,
            ExtraFields::Read => Some(ExtraFields::ReadOk {
                messages: node_data.messages.clone(),
            }),
            ExtraFields::ReadOk { messages: _ } => None,
            ExtraFields::Topology { topology } => {
                node_data.topology = Some(topology);
                Some(ExtraFields::TopologyOk)
            }
            ExtraFields::TopologyOk => None,
        };

        if let Some(inner) = ok_extra {
            let ok_msg = Msg {
                src: in_msg.dst,
                dst: in_msg.src,
                body: MsgBody {
                    msg_id,
                    in_reply_to: Some(in_msg.body.msg_id),
                    extra: inner,
                },
            };

            println!("{}", serde_json::to_string(&ok_msg)?);
        }
    }
}
