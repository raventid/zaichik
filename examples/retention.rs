use std::error::Error;
use zaichik;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut producer = zaichik::Client::connect("127.0.0.1:8889").await?;

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
    let mut consumer = zaichik::Client::connect("127.0.0.1:8889").await?;

    consumer.subscribe_on("hello".to_string()).await?;

    let message = consumer.read_message().await?;
    consumer.commit().await?;
    println!("Result is {:?}", message);

    Ok(())
}
