use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = zaichik::Client::connect("127.0.0.1:8889").await?;

    client
        .publish("hello".to_string(), "message".to_string().into_bytes())
        .await;
    client.subscribe_on("hello".to_string()).await;

    client.subscribe_on("W".to_string()).await;
    client.subscribe_on("t".to_string()).await;

    let result = client.read_message().await.unwrap();
    println!("Result is {:?}", result);

    Ok(())
}
