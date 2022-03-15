use bytes::Bytes;
use mini_redis::{
    Connection, 
    Frame,
    Command::{self, Get, Set}
};
use std::{
    collections::{HashMap, hash_map::DefaultHasher}, 
    hash::{Hash, Hasher}
};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type Shard = Mutex<HashMap<String, Bytes>>;
struct ShardedDb {
    shards: Vec<Shard>
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(ShardedDb::new(10));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = db.clone();

        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

trait Db {
    fn insert(&self, key: &str, value: Bytes);
    fn get(&self, key: &str) -> Option<Bytes>;
}

impl ShardedDb {
    pub fn new(num_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Mutex::new(HashMap::new()));
        }
    
        Self { shards }
    }

    fn find_shard(&self, key: &str) -> &Shard {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        let hashed_key = s.finish() as usize;

        &self.shards[hashed_key % self.shards.len()]
    }
}

impl Db for ShardedDb {
    fn insert(&self, key: &str, value: Bytes) {
        self.find_shard(key)
            .lock()
            .unwrap()
            .insert(key.to_string(), value.clone());
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        self.find_shard(key)
            .lock()
            .unwrap()
            .get(key)
            .map(|value| value.clone())
    }
}

async fn process(socket: TcpStream, db: Arc<ShardedDb>) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
