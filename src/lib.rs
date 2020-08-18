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

    pub async fn read_message(&mut self) -> Option<protocol::ZaichikFrame> {
        if let Some(Ok(frame)) = self.stream.next().await {
            Some(frame)
        } else {
            None
        }
    }

    pub async fn subscribe_on(&mut self, topic: String) {
        let frame = protocol::ZaichikFrame::Subscribe {
            topic: topic.clone(),
        };
        let _ = self.stream.send(frame).await;
        println!("Tried to send SUBSCRIBE message on topic {}", topic);
    }
}
