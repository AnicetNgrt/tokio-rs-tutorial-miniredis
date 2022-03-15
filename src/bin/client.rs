use mini_redis::client;
use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Debug)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        val: Bytes,
    }
}


#[tokio::main]
async fn main() {
    // let mut client = client::connect("127.0.0.1:6379").await.unwrap();

    let (tx, mut rx) = mpsc::channel(32);

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key } => {
                    client.get(&key).await.unwrap();
                }
                Command::Set { key, val } => {
                    client.set(&key, val).await.unwrap();
                }
            }
        }
    });
}