use futures::SinkExt;
use std::error::Error;
use tokio::stream::StreamExt;
mod protocol;

pub struct Client {
    stream: tokio_util::codec::Framed<tokio::net::TcpStream, protocol::ZaichikCodec>,
}

impl Client {
    pub async fn connect(server_addr: &str) -> Result<Client, Box<dyn Error>> {
        // let server_addr = "127.0.0.1:61616";
        println!("Connecting to {} ...", server_addr);

        let stream = tokio::net::TcpStream::connect(server_addr).await?;
        let framed = tokio_util::codec::Framed::new(stream, protocol::ZaichikCodec::new());

        println!("Established connection to {}", server_addr);

        Ok(Client { stream: framed })
    }

    pub async fn read_message(&mut self) -> Result<protocol::ZaichikFrame, std::io::Error> {
        self.stream.next().await.unwrap()
    }

    pub async fn create_topic(
        &mut self,
        topic: String,
        retention_ttl: u64,
        compaction_window: u64,
    ) -> Result<(), std::io::Error> {
        let frame = protocol::ZaichikFrame::CreateTopic {
            topic,
            retention_ttl,
            compaction_window,
        };

        self.stream.send(frame).await
    }

    pub async fn subscribe_on(&mut self, topic: String) -> Result<(), std::io::Error> {
        let frame = protocol::ZaichikFrame::Subscribe {
            topic: topic.clone(),
        };

        self.stream.send(frame).await
    }

    pub async fn publish(&mut self, topic: String, payload: Vec<u8>) -> Result<(), std::io::Error> {
        let frame = protocol::ZaichikFrame::Publish {
            topic,
            key: Some("secret".to_string()),
            payload,
        };

        self.stream.send(frame).await
    }

    pub async fn commit(&mut self) -> Result<(), std::io::Error> {
        let frame = protocol::ZaichikFrame::Commit {};

        self.stream.send(frame).await
    }

    pub async fn close(&mut self) -> Result<(), std::io::Error> {
        let frame = protocol::ZaichikFrame::CloseConnection {};

        self.stream.send(frame).await
    }
}
