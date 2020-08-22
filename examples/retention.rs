use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = std::env::vars()
        .find(|(key, _value)| key == "PORT")
        .map(|(_key, value)| value)
        .unwrap_or_else(|| "8889".to_string());

    let mut producer = zaichik::Client::connect(&format!("127.0.0.1:{}", port)).await?;

    producer
        .create_topic("hello".to_string(), 10_000, 0)
        .await?;

    producer
        .publish(
            "hello".to_string(),
            Some("key1".to_string()),
            "message".to_string().into_bytes(),
        )
        .await?;

    // Подключаемся уже после отправки сообщения продьюсером.
    let mut consumer = zaichik::Client::connect(&format!("127.0.0.1:{}", port)).await?;

    consumer.subscribe_on("hello".to_string()).await?;

    let message = consumer.read_message().await?;
    consumer.commit().await?;
    println!("Result is {:?}", message);

    Ok(())
}
